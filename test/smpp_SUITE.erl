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
     flow_control_in_flight].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ranch),
    Config.

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% test cases
%%%===================================================================
echo_smsc(_Config) ->
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    {ok, _} = test_esme_1:start(?ESME_REF, #{subscriber => self()}),
    SrcAddr = "1234",
    DestAddr = "88005553535",
    ShortMessage = "test case 1",
    SubmitSm = #submit_sm{source_addr = SrcAddr, destination_addr = DestAddr,
                          short_message = ShortMessage},
    {ok, _MessageId} = gen_esme:send(?ESME_REF, SubmitSm, timer:seconds(5)),
    [#deliver_sm{source_addr = DestAddr, destination_addr = SrcAddr,
                 short_message = ShortMessage}] =
        wait_for_events(timer:seconds(1)),
    ranch:stop_listener(?SMSC_REF).

rejecting_smsc(_Config) ->
    {ok, _} = faulty_smsc_1:start(?SMSC_REF, #{}),
    {ok, Pid} = test_esme_2:start(?ESME_REF,
                                  #{subscriber => self(),
                                    reconnect => false,
                                    bind_timeout => 500}),
    [{disconnected, {bind_failed, ?ESME_RBINDFAIL}, binding}] =
        wait_for_events(timer:seconds(1)),
    false = is_process_alive(Pid),
    ranch:stop_listener(?SMSC_REF).

bind_repair(_Config) ->
    {ok, _} = faulty_echo_smsc:start(?SMSC_REF, #{counter => counters:new(1, [])}),
    {ok, Pid} = test_esme_1:start(?ESME_REF,
                                  #{subscriber => self(),
                                    reconnect => true,
                                    bind_timeout => 500,
                                    reconnect_timeout => 100}),
    ct:sleep({seconds, 1}),
    true = is_process_alive(Pid),
    {bound, _} = sys:get_state(Pid),
    ranch:stop_listener(?SMSC_REF).

connection_repair(_Config) ->
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    {ok, EsmePid} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                                   reconnect_timeout => 100}),
    ct:sleep({seconds, 1}),
    ranch:stop_listener(?SMSC_REF),
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    ct:sleep({seconds, 1}),
    true = is_process_alive(EsmePid),
    {bound, _} = sys:get_state(EsmePid),
    ranch:stop_listener(?SMSC_REF).

flow_control_req_per_sec(_Config) ->
    {ok, _} = echo_smsc:start(?SMSC_REF, #{}),
    {global, Id} = ?ESME_REF,
    {ok, _} = test_esme_1:start(?ESME_REF, #{subscriber => self(),
                                             req_per_sec => 1,
                                             max_await_reqs => 1,
                                             id => Id}),
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
    ok = Send(),
    ranch:stop_listener(?SMSC_REF).

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
    {ok, {?ESME_ROK, #submit_sm_resp{}}} = gen_esme:send(?ESME_REF, SubmitSm, 1000),
    ranch:stop_listener(?SMSC_REF).


%%%===================================================================
%%% Internal functions
%%%===================================================================
wait_for_events(T) ->
    wait_for_events([], T).

wait_for_events(Acc, T) ->
    receive
        X ->
            [X | Acc]
    after
        T ->
            lists:reverse(Acc)
    end.
