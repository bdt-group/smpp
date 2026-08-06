-module(smpp_SUITE).

-include_lib("smpp34pdu/include/smpp34pdu.hrl").

-compile(export_all).

-define(SMSC_REF, {global, {smsc, ?FUNCTION_NAME}}).
-define(ESME_REF, {global, {esme, ?FUNCTION_NAME}}).

%%%===================================================================
%%% CT config
%%%===================================================================
suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [echo_smsc,
     rejecting_smsc,
     bind_repair,
     connection_repair,
     flow_control_req_per_sec,
     flow_control_in_flight,
     ignore_response_timeout,
     in_flight_cooldown_keepalive_no_hang,
     reconnect_on_stuck_inflight,
     reconnect_on_stuck_inflight_disabled,
     reconnect_on_stuck_inflight_not_triggered_when_responsive].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ranch),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, _Config) ->
    ranch:stop_listener({global, {smsc, TestCase}}),
    ok.

%%%===================================================================
%%% test cases
%%%===================================================================
echo_smsc(_Config) ->
    true = erlang:function_exported(test_esme_1, handle_data, 2),
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    {ok, _} = test_esme_1:start(?ESME_REF, #{subscriber => self()}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "test case 1",
    MessagePayload = list_to_binary(ShortMessage),
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage, vendor_specific = #{16#3fff => <<1,2>>}},
    {ok, _MessageId} = gen_esme:send(?ESME_REF, SubmitSm, timer:seconds(5)),
    [#deliver_sm{source_addr = DestAddr, destination_addr = SrcAddr,
                 short_message = ShortMessage}] =
        wait_for_events(timer:seconds(1)),

    CancelSm = #cancel_sm{message_id = "5462162233988800242",
                          source_addr = SrcAddr, destination_addr = DestAddr},
    {ok, {?ESME_ROK, #cancel_sm_resp{}}} = gen_esme:send(?ESME_REF, CancelSm, timer:seconds(5)),

    DataSm = #data_sm{source_addr = SrcAddr, destination_addr = DestAddr, message_payload = MessagePayload},
    {ok, {?ESME_ROK, #data_sm_resp{}}} = gen_esme:send(?ESME_REF, DataSm, timer:seconds(2)),
    [#data_sm{source_addr = DestAddr, destination_addr = SrcAddr,
              message_payload = MessagePayload}] =
        wait_for_events(timer:seconds(1)),

    [SMSCPid] = ranch:procs(?SMSC_REF, connections),
    SMSCConnState = sys:get_state(SMSCPid),

    ct:pal("SMSCState: ~p", [SMSCConnState]),
    {bound, #{peer_addr := {127,0,0,1}, peer_port := PeerPort}} = SMSCConnState,
    true = (PeerPort =/= 2775).

rejecting_smsc(_Config) ->
    {ok, _} = faulty_smsc_1:start(?SMSC_REF, #{}),
    {ok, Pid} = test_esme_2:start(?ESME_REF,
                                  #{subscriber => self(),
                                    reconnect => false,
                                    bind_timeout => 500}),
    [{disconnected, {bind_failed, ?ESME_RBINDFAIL}, binding}] =
        wait_for_events(timer:seconds(1)),
    false = is_process_alive(Pid).

bind_repair(_Config) ->
    {ok, _} = faulty_echo_smsc:start(?SMSC_REF, #{counter => counters:new(1, [])}),
    {ok, Pid} = test_esme_1:start(?ESME_REF,
                                  #{subscriber => self(),
                                    reconnect => true,
                                    bind_timeout => 500,
                                    reconnect_timeout => 100}),
    ct:sleep({seconds, 1}),
    true = is_process_alive(Pid),
    {bound, _} = sys:get_state(Pid).

connection_repair(_Config) ->
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    {ok, EsmePid} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                                   reconnect_timeout => 100}),
    ct:sleep({seconds, 1}),
    ranch:stop_listener(?SMSC_REF),
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    ct:sleep({seconds, 1}),
    true = is_process_alive(EsmePid),
    {bound, _} = sys:get_state(EsmePid).

flow_control_req_per_sec(_Config) ->
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    {ok, _} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                             req_per_sec => 1,
                                             max_await_reqs => 1}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "test case 1",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    Send =
        fun() ->
                gen_esme:send_async(?ESME_REF,
                                    SubmitSm,
                                    fun(_, S) -> S end,
                                    timer:seconds(1))
        end,
    ok = Send(),
    {error, overload} = Send(),
    {error, overload} = Send(),
    ct:sleep(1100),
    ok = Send().

flow_control_in_flight(_Config) ->
    {ok, _} = slow_echo_smsc:start(?SMSC_REF, #{}),
    {global, Id} = ?ESME_REF,
    {ok, _} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                             in_flight_limit => 0,
                                             id => Id}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "test case 1",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    ok = gen_esme:send_async(?ESME_REF,
                             SubmitSm,
                             fun(_, S) -> S end,
                             1000),
    ok = gen_esme:send_async(?ESME_REF,
                             SubmitSm,
                             fun({error, sending_expired}, State) -> State end,
                             500),
    ct:sleep(1000),
    {ok, {?ESME_ROK, #submit_sm_resp{}}} = gen_esme:send(?ESME_REF, SubmitSm, 1000),
    {ok, {?ESME_ROK, #submit_sm_resp{}}} = gen_esme:send(?ESME_REF, SubmitSm, 1000).

ignore_response_timeout(_Config) ->
    {ok, _} = slow_echo_smsc:start(?SMSC_REF, #{}),
    {global, Id} = ?ESME_REF,
    {ok, EsmePid} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                                   ignore_response_timeout => true,
                                                   response_timeout => 600,
                                                   reconnect => false,
                                                   in_flight_limit => 10,
                                                   id => Id}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "test case 1",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    {error, timeout} = gen_esme:send(?ESME_REF, SubmitSm, 600),
    true = erlang:is_process_alive(EsmePid).

in_flight_cooldown_keepalive_no_hang(_Config) ->
    {ok, _} = delayed_echo_smsc:start(?SMSC_REF, #{delay => 800}),
    {global, Id} = ?ESME_REF,
    {ok, EsmePid} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                                   in_flight_limit => 0,
                                                   keepalive_timeout => 150,
                                                   response_timeout => 3000,
                                                   id => Id}),
    timer:sleep(100),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "keepalive during in flight cooldown",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    ok = gen_esme:send_async(?ESME_REF, SubmitSm, fun(_, S) -> S end, 5000),
    {ok, {?ESME_ROK, #submit_sm_resp{}}} = gen_esme:send(?ESME_REF, SubmitSm, 5000),
    true = erlang:is_process_alive(EsmePid),
    {bound, _} = sys:get_state(EsmePid).

reconnect_on_stuck_inflight(_Config) ->
    {ok, _} = black_hole_smsc:start(?SMSC_REF, #{}),
    {global, Id} = ?ESME_REF,
    {ok, EsmePid} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                                   in_flight_limit => 0,
                                                   response_timeout => 300,
                                                   reconnect_timeout => 100,
                                                   reconnect => true,
                                                   reconnect_on_stuck_inflight => true,
                                                   id => Id}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "swallowed forever",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    ok = gen_esme:send_async(?ESME_REF, SubmitSm, fun(_, S) -> S end, 5000),
    ok = gen_esme:send_async(?ESME_REF, SubmitSm, fun(_, S) -> S end, 5000),
    ct:sleep(1500),
    true = erlang:is_process_alive(EsmePid),
    {bound, #{in_flight := []}} = sys:get_state(EsmePid). %on reconnect in_flight is reset

reconnect_on_stuck_inflight_disabled(_Config) ->
    {ok, _} = black_hole_smsc:start(?SMSC_REF, #{}),
    {global, Id} = ?ESME_REF,
    {ok, EsmePid} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                                   in_flight_limit => 0,
                                                   response_timeout => 300,
                                                   reconnect => true,
                                                   id => Id}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "swallowed forever",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    ok = gen_esme:send_async(?ESME_REF, SubmitSm, fun(_, S) -> S end, 5000),
    ok = gen_esme:send_async(?ESME_REF, SubmitSm, fun(_, S) -> S end, 5000),
    ct:sleep(1500),
    true = erlang:is_process_alive(EsmePid),
    {in_flight_limit_cooldown, _} = sys:get_state(EsmePid).

reconnect_on_stuck_inflight_not_triggered_when_responsive(_Config) ->
    {ok, _} = delayed_echo_smsc:start(?SMSC_REF, #{delay => 300}),
    {global, Id} = ?ESME_REF,
    {ok, EsmePid} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                                   in_flight_limit => 0,
                                                   response_timeout => 5000,
                                                   reconnect_on_stuck_inflight => true,
                                                   id => Id}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "not actually stuck",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    ct:sleep(200),
    St1 = {bound, #{socket := S1, seq := 1}} = sys:get_state(EsmePid),
    ct:pal("State: ~p", [St1]),
    ok = gen_esme:send_async(?ESME_REF, SubmitSm, fun(_, S) -> S end, 5000),
    {ok, {?ESME_ROK, #submit_sm_resp{}}} = gen_esme:send(?ESME_REF, SubmitSm, 5000),
    true = erlang:is_process_alive(EsmePid),
    St2 = {bound, #{socket := S2, seq := 3}} = sys:get_state(EsmePid),
    ct:pal("State2: ~p", [St2]),
    S1 = S2. % sockets change on reconnect


%%%===================================================================
%%% Internal functions
%%%===================================================================
wait_for_events(T) ->
    wait_for_events([], T).

wait_for_events(Acc, T) ->
    receive
        X ->
            wait_for_events([X | Acc], T)
    after
        T ->
            lists:reverse(Acc)
    end.