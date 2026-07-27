%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(local_dynamic_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(shovel_test_utils, [with_amqp10_session/2, with_amqp10_session/3,
                            amqp10_expect_empty/2,
                            amqp10_publish/4, amqp10_expect_one/2,
                            amqp10_expect_count/3, amqp10_expect/3,
                            amqp10_publish_expect/5, amqp10_subscribe/2,
                            amqp10_declare_queue/3,
                            await_autodelete/2]).

-define(PARAM, <<"test">>).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  local_to_local_opt_headers,
                  local_to_local_stream_no_ack,
                  local_to_local_delete_dest_queue,
                  local_to_local_stream_credit_flow_no_ack,
                  local_to_local_simple_uri,
                  local_to_local_counters,
                  local_to_local_alarms,
                  backpressure_non_confirm_blocks_unblocks,
                  backpressure_quorum_queue_blocks_and_unblocks,
                  local_to_local_quorum_on_publish,
                  local_to_local_quorum_no_ack
                 ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
        {rmq_nodename_suffix, ?MODULE},
      {ignored_crashes, [
          "server_initiated_close,404",
          "writer,send_failed,closed",
          "source_queue_down",
          "dest_queue_down"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    [Node] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(
           Config, [Node], 'rabbitmq_4.0.0'),
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    SrcQ = list_to_binary(atom_to_list(Testcase) ++ "_src"),
    DestQ = list_to_binary(atom_to_list(Testcase) ++ "_dest"),
    DestQ2 = list_to_binary(atom_to_list(Testcase) ++ "_dest2"),
    VHost = list_to_binary(atom_to_list(Testcase) ++ "_vhost"),
    Config = [{srcq, SrcQ}, {destq, DestQ}, {destq2, DestQ2},
              {alt_vhost, VHost} | Config0],

    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    shovel_test_utils:clear_param(Config, ?PARAM),
    rabbit_ct_broker_helpers:rpc(Config, 0, shovel_test_utils, delete_all_queues, []),
    _ = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(alt_vhost, Config)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

local_to_local_opt_headers(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-add-forward-headers">>, true},
                                           {<<"dest-add-timestamp-header">>, true}
                                          ]),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              DestAddress = rabbitmq_amqp_address:queue(Dest),
              [Msg] = amqp10_publish_expect(Sess, SrcAddress, DestAddress, <<"hello">>, 1),
              ?assertMatch(#{<<"x-opt-shovel-name">> := ?PARAM,
                             <<"x-opt-shovel-type">> := <<"dynamic">>,
                             <<"x-opt-shovelled-by">> := _,
                             <<"x-opt-shovelled-timestamp">> := _},
                           amqp10_msg:message_annotations(Msg))
      end).

local_to_local_stream_no_ack(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, <<"/">>, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"no-ack">>}
                                          ]),
              DestAddress = rabbitmq_amqp_address:queue(Dest),
              Receiver = amqp10_subscribe(Sess, DestAddress),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              amqp10_publish(Sess, SrcAddress, <<"tag1">>, 10),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 10}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 10, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_delete_dest_queue(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              DestAddress = rabbitmq_amqp_address:queue(Dest),
              _ = amqp10_publish_expect(Sess, SrcAddress, DestAddress, <<"hello">>, 1),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queue,
                                           [Dest, <<"/">>]),
              ?awaitMatch([{_Name, dynamic, {terminated, dest_queue_down}, _, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000)
      end).

local_to_local_stream_credit_flow_no_ack(Config) ->
    local_to_local_stream_credit_flow(Config, <<"no-ack">>).

local_to_local_stream_credit_flow(Config, AckMode) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-predeclared">>, true},
                                           {<<"ack-mode">>, AckMode}
                                          ]),
              DestAddress = rabbitmq_amqp_address:queue(Dest),
              Receiver = amqp10_subscribe(Sess, DestAddress),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              amqp10_publish(Sess, SrcAddress, <<"tag1">>, 1000),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1000}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 1000, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_simple_uri(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Uri = <<"amqp://">>,
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set,
           [<<"/">>, <<"shovel">>, ?PARAM, [{<<"src-uri">>,  Uri},
                                            {<<"dest-uri">>, [Uri]},
                                            {<<"src-protocol">>, <<"local">>},
                                            {<<"src-queue">>, Src},
                                            {<<"dest-protocol">>, <<"local">>},
                                            {<<"dest-queue">>, Dest}],
            none]),
    shovel_test_utils:await_shovel(Config, ?PARAM).

local_to_local_counters(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    %% Let's restart the node so the counters are reset
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              ?awaitMatch(#{publishers := 0, consumers := 0},
                          get_global_counters(Config), 30_000),
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              ?awaitMatch(#{publishers := 1, consumers := 1},
                          get_global_counters(Config), 30_000),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              _ = amqp10_publish(Sess, SrcAddress, <<"tag1">>, 150),
              ?awaitMatch(#{consumers := 1, publishers := 1,
                            messages_received_total := 150,
                            messages_received_confirm_total := 150,
                            messages_routed_total := 150,
                            messages_unroutable_dropped_total := 0,
                            messages_unroutable_returned_total := 0,
                            messages_confirmed_total := 150},
                          get_global_counters(Config), 30_000)
      end).

local_to_local_alarms(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    ShovelArgs = [{<<"src-protocol">>, <<"local">>},
                  {<<"src-queue">>, Src},
                  {<<"dest-protocol">>, <<"local">>},
                  {<<"dest-queue">>, Dest}],
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_declare_queue(Sess, Src, #{}),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              amqp10_publish(Sess, SrcAddress, <<"hello">>, 1000),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, disk),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, disk),
              shovel_test_utils:set_param(Config, ?PARAM, ShovelArgs),
              ?awaitMatch({running, blocked}, get_blocked_status(Config), 30000),
              DestAddress = rabbitmq_amqp_address:queue(Dest),
              amqp10_expect_empty(Sess, DestAddress),
              rabbit_ct_broker_helpers:clear_alarm(Config, 0, disk),
              ?awaitMatch({running, running}, get_blocked_status(Config), 30000),
              amqp10_expect_count(Sess, DestAddress, 1000),

              shovel_test_utils:clear_param(Config, ?PARAM),

              amqp10_publish(Sess, SrcAddress, <<"hello">>, 1000),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, disk),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, memory),
              shovel_test_utils:set_param(Config, ?PARAM, ShovelArgs),
              ?awaitMatch({running, blocked}, get_blocked_status(Config), 30000),
              amqp10_expect_empty(Sess, DestAddress),
              rabbit_ct_broker_helpers:clear_alarm(Config, 0, disk),
              ?awaitMatch({running, blocked}, get_blocked_status(Config), 30000),
              amqp10_expect_empty(Sess, DestAddress),
              rabbit_ct_broker_helpers:clear_alarm(Config, 0, memory),
              ?awaitMatch({running, running}, get_blocked_status(Config), 30000),
              amqp10_expect_count(Sess, DestAddress, 1000)
      end).
local_to_local_quorum_on_publish(Config) ->
    local_to_local_quorum(Config, <<"on-publish">>).

local_to_local_quorum_no_ack(Config) ->
    local_to_local_quorum(Config, <<"no-ack">>).

backpressure_non_confirm_blocks_unblocks(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, ?MODULE, backpressure_non_confirm_blocks_unblocks1, []).

backpressure_non_confirm_blocks_unblocks1() ->
    S0 = #{source => #{},
           dest => #{blocked_queues => sets:new(),
                     alarms => sets:new(),
                     pending_delivery => lqueue:new()}},
    S1 = rabbit_local_shovel:handle_dest_queue_actions(
           [{block, <<"q1">>}], S0),
    true = sets:is_element(
             <<"q1">>,
             maps:get(blocked_queues, maps:get(dest, S1))),
    S2 = rabbit_local_shovel:handle_dest_queue_actions(
           [{unblock, <<"q1">>}], S1),
    true = sets:is_empty(
            maps:get(blocked_queues, maps:get(dest, S2))),
    ok.

local_to_local_quorum(Config, AckMode) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, []),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-predeclared">>, true},
                                           {<<"ack-mode">>, AckMode}
                                          ]),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              DestAddress = rabbitmq_amqp_address:queue(Dest),
              Receiver = amqp10_subscribe(Sess, DestAddress),
              amqp10_publish(Sess, SrcAddress, <<"tag1">>, 1000),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1000}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 1000, []),
              amqp10_client:detach_link(Receiver)
      end).

backpressure_quorum_queue_blocks_and_unblocks(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, []),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    %% Lower the soft limit to 1 so {block, QName} fires on every
    %% pending command, making the block state reliably observable.
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, application, set_env,
           [rabbit, quorum_commands_soft_limit, 1]),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-predeclared">>, true},
                                           {<<"ack-mode">>, <<"on-confirm">>}
                                          ]),
              SrcAddress = rabbitmq_amqp_address:queue(Src),
              DestAddress = rabbitmq_amqp_address:queue(Dest),
              Receiver = amqp10_subscribe(Sess, DestAddress),
              amqp10_publish(Sess, SrcAddress, <<"tag1">>, 100),
              %% Poll for blocked status while forwarding is in progress.
              %% On main without the fix: is_blocked only checks alarms,
              %% so blocked is never returned — WasBlocked stays false.
              %% On the fix branch: is_blocked also checks blocked_queues,
              %% so blocked is seen after every message sent.
              WasBlocked = poll_until(
                             fun() ->
                                     case get_blocked_status(Config) of
                                         {running, blocked} -> true;
                                         _                  -> false
                                     end
                             end, 30000),
              ?assert(WasBlocked),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 100}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 100, []),
              amqp10_client:detach_link(Receiver)
      end).

poll_until(Pred, Timeout) when Timeout > 0 ->
    Start = erlang:monotonic_time(millisecond),
    poll_until_loop(Pred, Start, Timeout).

poll_until_loop(Pred, Start, Timeout) ->
    case Pred() of
        true ->
            true;
        false ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            case Elapsed < Timeout of
                true ->
                    timer:sleep(100),
                    poll_until_loop(Pred, Start, Timeout);
                false ->
                    false
            end
    end.
%%----------------------------------------------------------------------------
declare_queue(Config, VHost, QName) ->
    declare_queue(Config, VHost, QName, []).

declare_queue(Config, VHost, QName, Args) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch, #'queue.declare'{queue = QName, durable = true, arguments = Args})),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

declare_and_bind_queue(Config, VHost, Exchange, QName, RoutingKey) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch, #'queue.declare'{queue = QName, durable = true,
                              arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]})),
    ?assertMatch(
       #'queue.bind_ok'{},
       amqp_channel:call(Ch, #'queue.bind'{
                                queue = QName,
                                exchange = Exchange,
                                routing_key = RoutingKey
                               })),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

declare_exchange(Config, VHost, Exchange) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertMatch(
       #'exchange.declare_ok'{},
       amqp_channel:call(Ch, #'exchange.declare'{exchange = Exchange})),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

delete_queue(Name, VHost) ->
    QName = rabbit_misc:r(VHost, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.

get_global_counters(Config) ->
    get_global_counters0(Config, #{protocol => 'local-shovel'}).

get_global_counters0(Config, Key) ->
    Overview = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_global_counters, overview, []),
    maps:get(Key, Overview).

get_blocked_status(Config) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status, status, []) of
        [{_, _, {Status, PropList}, _, _}] ->
            {Status, proplists:get_value(blocked_status, PropList)};
        _ ->
            empty
    end.
