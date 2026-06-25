%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved.

-module(rabbit_logger_fmt_helpers_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, redaction},
      {group, formatter_integration}
    ].

groups() ->
    [
      {redaction, [parallel], [
          redacts_amqp_credentials,
          redacts_amqps_credentials,
          redacts_mqtt_credentials,
          redacts_stomp_credentials,
          leaves_non_uri_unchanged,
          leaves_uri_without_credentials_unchanged,
          handles_multiple_uris_in_one_message,
          handles_empty_message,
          handles_malformed_uri_gracefully
        ]},
      {formatter_integration, [parallel], [
          format_msg_with_report_redacts_uris,
          format_msg_with_string_passes_through,
          format_msg_with_supervisor_progress_report
        ]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.
init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

%% ---- Unit tests for redact_credentials/1 ----

redacts_amqp_credentials(_Config) ->
    Input = "Supervisor child started: {rabbit_shovel_worker,start_link,"
            "[static,{<<\"/\">>,<<\"dummy\">>},"
            "[{<<\"dest-uri\">>,<<\"amqp://user:pass@localhost:5672\">>}]]}",
    Expected = "Supervisor child started: {rabbit_shovel_worker,start_link,"
               "[static,{<<\"/\">>,<<\"dummy\">>},"
               "[{<<\"dest-uri\">>,<<\"amqp://****:****@localhost:5672\">>}]]}",
    ?assertEqual(Expected, rabbit_logger_fmt_helpers:redact_credentials(Input)).

redacts_amqps_credentials(_Config) ->
    Input = "amqps://admin:s3cret@remotehost:5671/vhost",
    Expected = "amqps://****:****@remotehost:5671/vhost",
    ?assertEqual(Expected, rabbit_logger_fmt_helpers:redact_credentials(Input)).

redacts_mqtt_credentials(_Config) ->
    Input = "mqtt://device:token@broker.local:1883",
    Expected = "mqtt://****:****@broker.local:1883",
    ?assertEqual(Expected, rabbit_logger_fmt_helpers:redact_credentials(Input)).

redacts_stomp_credentials(_Config) ->
    Input = "stomp://user:pass@stomp-broker:61613",
    Expected = "stomp://****:****@stomp-broker:61613",
    ?assertEqual(Expected, rabbit_logger_fmt_helpers:redact_credentials(Input)).

leaves_non_uri_unchanged(_Config) ->
    Input = "connection <0.1.0>: user 'guest' authenticated and granted access to vhost '/'",
    ?assertEqual(Input, rabbit_logger_fmt_helpers:redact_credentials(Input)).

leaves_uri_without_credentials_unchanged(_Config) ->
    Input = "amqp://localhost:5672",
    ?assertEqual(Input, rabbit_logger_fmt_helpers:redact_credentials(Input)).

handles_multiple_uris_in_one_message(_Config) ->
    Input = "src: amqp://x:x@host1:5672, dest: amqp://y:y@host2:5672",
    Expected = "src: amqp://****:****@host1:5672, dest: amqp://****:****@host2:5672",
    ?assertEqual(Expected, rabbit_logger_fmt_helpers:redact_credentials(Input)).

handles_empty_message(_Config) ->
    ?assertEqual("", rabbit_logger_fmt_helpers:redact_credentials("")),
    ?assertEqual(<<>>, rabbit_logger_fmt_helpers:redact_credentials(<<>>)).

handles_malformed_uri_gracefully(_Config) ->
    Input = "amqp://user@localhost:5672",
    ?assertEqual(Input, rabbit_logger_fmt_helpers:redact_credentials(Input)).

%% ---- Formatter integration tests ----

format_msg_with_report_redacts_uris(_Config) ->
    Report = #{label => {supervisor, progress},
                report => [{supervisor, {list_to_pid("<0.1.0>"), rabbit_shovel_worker_sup}},
                          {started, [{id, {<<"/">>, <<"dummy">>}},
                                     {pid, list_to_pid("<0.2.0>")},
                                     {mfargs,
                                      {rabbit_shovel_worker, start_link,
                                       [static,
                                        {<<"/">>, <<"dummy">>},
                                        [{<<"src-uri">>,
                                          <<"amqp://user:password@source:5672">>},
                                         {<<"dest-uri">>,
                                          <<"amqp://user2:secret@dest:5672">>}]]}}]}]},
    Msg = {report, Report},
    Meta = #{},
    Config = #{},
    Result = rabbit_logger_fmt_helpers:format_msg(Msg, Meta, Config),
    ResultStr = iolist_to_binary(lists:flatten(Result)),
    ?assertEqual(nomatch, binary:match(ResultStr, <<"user:password">>)),
    ?assertEqual(nomatch, binary:match(ResultStr, <<"user2:secret">>)),
    ?assert(binary:match(ResultStr, <<"****:****">>) =/= nomatch).

format_msg_with_string_passes_through(_Config) ->
    Msg = {string, "connection <0.1.0>: user 'guest' authenticated"},
    Meta = #{},
    Config = #{},
    Result = rabbit_logger_fmt_helpers:format_msg(Msg, Meta, Config),
    ResultStr = iolist_to_binary(lists:flatten(Result)),
    ?assert(binary:match(ResultStr, <<"guest">>) =/= nomatch).

format_msg_with_supervisor_progress_report(_Config) ->
    Report = #{label => {supervisor, progress},
               report => [{supervisor, {list_to_pid("<0.1.0>"), rabbit_sup}},
                          {started, [{id, some_child},
                                     {pid, list_to_pid("<0.2.0>")},
                                     {mfargs, {some_module, start_link, []}}]}]},
    Msg = {report, Report},
    Meta = #{},
    Config = #{},
    Result = rabbit_logger_fmt_helpers:format_msg(Msg, Meta, Config),
    ResultStr = iolist_to_binary(lists:flatten(Result)),
    ?assert(binary:match(ResultStr, <<"some_child">>) =/= nomatch),
    ?assertEqual(nomatch, binary:match(ResultStr, <<"****">>)).
