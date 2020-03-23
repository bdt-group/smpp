%%%-------------------------------------------------------------------
%%% @author xram <xram@xram>
%%% @doc
%%%
%%% @end
%%% Created : 27 Feb 2020 by xram <xram@xram>
%%%-------------------------------------------------------------------
-module(smpp_socket).

-behaviour(gen_statem).
-behaviour(ranch_protocol).

%% API
-export([child_spec/4]).
-export([connect/1, connect/2]).
-export([connect_link/1, connect_link/2]).
-export([listen/3]).
-export([start_link/2, start_link/3]).
-export([send/3]).
-export([send_async/2, send_async/3, send_async/4]).
-export([format_error/1]).
-export([pp/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([connecting/3, binding/3, bound/3]).

-include("smpp.hrl").
-include_lib("kernel/include/logger.hrl").

-define(VSN, 1).
-define(MAX_BUF_SIZE, 104857600).
-define(is_request(CmdID), ((CmdID band 16#80000000) == 0)).
-define(is_response(CmdID), ((CmdID band 16#80000000) /= 0)).
-define(is_receiver(Mode), (Mode == receiver orelse Mode == transceiver)).

-type state() :: #{vsn := non_neg_integer(),
                   role := peer_role(),
                   rq := request_queue(),
                   rq_size := non_neg_integer(),
                   rq_limit := pos_integer() | infinity,
                   in_flight => in_flight_request(),
                   callback => module(),
                   mode => mode(),
                   host => string(),
                   port => inet:port_number(),
                   system_id => string(),
                   password => string(),
                   bind_timeout => millisecs(),
                   connect_timeout => millisecs(),
                   reconnect_timeout => millisecs(),
                   keepalive_timeout => millisecs(),
                   keepalive_time => millisecs(),
                   response_time => millisecs(),
                   last_overload_report => millisecs(),
                   buf => binary(),
                   seq => 1..16#7FFFFFFF,
                   socket => port(),
                   transport => module(),
                   _ => term()}.

-type statename() :: connecting | binding | bound.
-type peer_role() :: smsc | esme.
-type mode() :: transmitter | receiver | transceiver.
-type millisecs() :: pos_integer().
-type codec_error_reason() :: {bad_length, non_neg_integer()} |
                              {bad_body, non_neg_integer()}.
-type error_reason() :: closed | timeout | unbinded |
                        overloaded | system_shutdown | system_error |
                        {inet, inet:posix()} |
                        {bind_failed, pos_integer()} |
                        codec_failure |
                        {codec_failure, codec_error_reason()}.
-type socket_name() :: {global, term()} | {via, module(), term()} | {local, atom()}.
-type sender() :: gen_statem:from() | send_callback() | undefined.
-type in_flight_request() :: {millisecs(), sender()}.
-type queued_request() :: {millisecs(), valid_pdu(), sender()}.
-type request_queue() :: queue:queue(queued_request()).
-type send_reply() :: {ok, {non_neg_integer(), valid_pdu()}} |
                      {error, error_reason()}.
-type send_callback() :: fun((send_reply(), state()) -> state()).

-export_type([error_reason/0, send_reply/0, statename/0, state/0, send_callback/0]).

%%%===================================================================
%%% API
%%%===================================================================
-spec child_spec(smsc, term(), state(), ranch_tcp:opts()) -> supervisor:child_spec();
                (esme, term(), state(), socket_name()) -> supervisor:child_spec().
child_spec(smsc, Id, State, RanchOpts) ->
    State1 = init_state(State, smsc),
    ranch:child_spec(Id, ranch_tcp, RanchOpts, ?MODULE, State1);
child_spec(esme, Id, State, Name) ->
    State1 = init_state(State, esme),
    #{id => Id,
      start => {?MODULE, start_link, [Name, State1]},
      restart => transient,
      type => worker,
      shutdown => timer:seconds(30),
      modules => [?MODULE]}.

-spec connect(state()) -> {ok, pid()} | {error, term()}.
connect(State) ->
    State1 = init_state(State, esme),
    gen_statem:start(?MODULE, State1, []).

-spec connect(socket_name(), state()) -> {ok, pid()} | {error, term()}.
connect(Name, State) ->
    State1 = init_state(State, esme),
    gen_statem:start(Name, ?MODULE, State1, []).

-spec connect_link(state()) -> {ok, pid()} | {error, term()}.
connect_link(State) ->
    State1 = init_state(State, esme),
    gen_statem:start_link(?MODULE, State1, []).

-spec connect_link(socket_name(), state()) -> {ok, pid()} | {error, term()}.
connect_link(Name, State) ->
    State1 = init_state(State, esme),
    gen_statem:start_link(Name, ?MODULE, State1, []).

-spec listen(term(), state(), ranch_tcp:opts()) -> {ok, pid()} | {error, term()}.
listen(Id, State, RanchOpts) ->
    State1 = init_state(State, smsc),
    ranch:start_listener(Id, ranch_tcp, RanchOpts, ?MODULE, State1).

-spec start_link(socket_name(), state()) -> {ok, pid()} | {error, term()}.
start_link(Name, State) ->
    gen_statem:start_link(Name, ?MODULE, State, []).

-spec start_link(ranch:ref(), module(), state()) -> {ok, pid()}.
start_link(Ref, Transport, State) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, State}])}.

-spec send_async(gen_statem:server_ref(), valid_pdu()) -> ok.
send_async(Ref, Pkt) ->
    send_async(Ref, Pkt, undefined).

-spec send_async(gen_statem:server_ref(), valid_pdu(), undefined | send_callback()) -> ok.
send_async(Ref, Pkt, Fun) ->
    gen_statem:cast(Ref, {send_req, Pkt, Fun, undefined}).

-spec send_async(gen_statem:server_ref(), valid_pdu(), undefined | send_callback(), millisecs()) -> ok.
send_async(Ref, Pkt, Fun, Timeout) ->
    Time = current_time() + Timeout,
    gen_statem:cast(Ref, {send_req, Pkt, Fun, Time}).

-spec send(gen_statem:server_ref(), valid_pdu(), millisecs()) -> send_reply().
send(Ref, Pkt, Timeout) ->
    Time = current_time() + Timeout,
    try gen_statem:call(Ref, {send_req, Pkt, Time}, {dirty_timeout, Timeout})
    catch exit:{timeout, {gen_statem, call, _}} ->
            {error, timeout};
          exit:{_, {gen_statem, call, _}} ->
            {error, closed}
    end.

-spec format_error(error_reason()) -> string().
format_error({inet, Reason}) ->
    case inet:format_error(Reason) of
        "unknown POSIX error" -> atom_to_list(Reason);
        S -> S
    end;
format_error(closed) ->
    "connection closed";
format_error(timeout) ->
    "timed out";
format_error(overloaded) ->
    "request queue is overfilled";
format_error(system_shutdown) ->
    "system is shutting down";
format_error(system_error) ->
    "internal system error";
format_error(codec_failure) ->
    "SMPP codec error";
format_error({codec_failure, {bad_length, Len}}) ->
    "SMPP codec error: invalid PDU length: " ++ integer_to_list(Len);
format_error({codec_failure, {bad_body, Id}}) ->
    "SMPP codec error: malformed body of command " ++ integer_to_list(Id);
format_error(unbinded) ->
    "binding closed by peer";
format_error({bind_failed, Status}) ->
    "binding failed (status = " ++ integer_to_list(Status) ++ ")".

-spec pp(any()) -> iolist().
pp(Term) ->
    io_lib_pretty:print(Term, fun pp/2).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> state_functions.

-spec init({ranch:ref(), module(), state()}) -> no_return() | ignore;
          (state()) -> gen_statem:init_result(connecting).
init({Ref, Transport, State}) ->
    case ranch:handshake(Ref) of
        {ok, Socket} ->
            case Transport:sockname(Socket) of
                {ok, {_, Port}} ->
                    State1 = State#{port => Port,
                                    socket => Socket,
                                    transport => Transport},
                    sock_activate(State1),
                    {_, StateName, State2, Timeout} = bind(State1),
                    State3 = callback(handle_connected, State2),
                    gen_statem:enter_loop(?MODULE, [], StateName, State3, [Timeout]);
                {error, _} ->
                    ignore
            end;
        _ ->
            ignore
    end;
init(State) ->
    self() ! connect,
    {ok, connecting, State}.

-spec connecting(gen_statem:event_type(), term(), state()) ->
                        gen_statem:event_handler_result(statename()).
connecting(info, connect, State) ->
    case sock_connect(State) of
        {ok, State1} ->
            State2 = callback(handle_connected, State1),
            bind(State2);
        {error, Reason} ->
            reconnect({inet, Reason}, ?FUNCTION_NAME, State)
    end;
connecting(state_timeout, _, State) ->
    connecting(info, connect, State);
connecting({call, From}, {send_req, Body, Time}, State) ->
    State1 = enqueue_req(State, Body, From, Time),
    next_state(?FUNCTION_NAME, State1);
connecting(cast, {send_req, Body, Fun, Time}, State) ->
    State1 = enqueue_req(State, Body, Fun, Time),
    next_state(?FUNCTION_NAME, State1);
connecting(EventType, Msg, State) ->
    report_unexpected_msg(EventType, Msg, ?FUNCTION_NAME, State),
    next_state(?FUNCTION_NAME, State).

-spec binding(gen_statem:event_type(), term(), state()) ->
                     gen_statem:event_handler_result(statename()).
binding(info, {tcp, Sock, Data}, #{socket := Sock} = State) ->
    decode(Data, ?FUNCTION_NAME, State);
binding(info, {tcp_closed, Sock}, #{socket := Sock} = State) ->
    reconnect(closed, ?FUNCTION_NAME, State);
binding(info, {tcp_error, Sock, Reason}, #{socket := Sock} = State) ->
    reconnect({inet, Reason}, ?FUNCTION_NAME, State);
binding(state_timeout, _, State) ->
    reconnect(timeout, ?FUNCTION_NAME, State);
binding({call, From}, {send_req, Body, Time}, State) ->
    State1 = enqueue_req(State, Body, From, Time),
    next_state(?FUNCTION_NAME, State1);
binding(cast, {send_req, Body, Fun, Time}, State) ->
    State1 = enqueue_req(State, Body, Fun, Time),
    next_state(?FUNCTION_NAME, State1);
binding(EventType, Msg, State) ->
    report_unexpected_msg(EventType, Msg, ?FUNCTION_NAME, State),
    next_state(?FUNCTION_NAME, State).

-spec bound(gen_statem:event_type(), term(), state()) ->
                       gen_statem:event_handler_result(statename()).
bound(info, {tcp, Sock, Data}, #{socket := Sock} = State) ->
    decode(Data, ?FUNCTION_NAME, State);
bound(info, {tcp_closed, Sock}, #{socket := Sock} = State) ->
    reconnect(closed, ?FUNCTION_NAME, State);
bound(info, {tcp_error, Sock, Reason}, #{socket := Sock} = State) ->
    reconnect({inet, Reason}, ?FUNCTION_NAME, State);
bound(timeout, keepalive, #{role := esme} = State) ->
    State1 = send_req(State, #enquire_link{}),
    next_state(?FUNCTION_NAME, State1);
bound(timeout, _, State) ->
    reconnect(timeout, ?FUNCTION_NAME, State);
bound({call, From}, {send_req, Body, Time}, State) ->
    State1 = send_req(State, Body, From, Time),
    next_state(?FUNCTION_NAME, State1);
bound(cast, {send_req, Body, Fun, Time}, State) ->
    State1 = send_req(State, Body, Fun, Time),
    next_state(?FUNCTION_NAME, State1);
bound(EventType, Msg, State) ->
    report_unexpected_msg(EventType, Msg, ?FUNCTION_NAME, State),
    next_state(?FUNCTION_NAME, State).

-spec terminate(term(), statename(), state()) -> any().
terminate(Why, StateName, State) ->
    Reason = case Why of
                 normal -> closed;
                 shutdown -> system_shutdown;
                 {shutdown, _} -> system_shutdown;
                 _ -> system_error
             end,
    State1 = discard_in_flight_request(Reason, State),
    State2 = discard_all_requests(Reason, State1),
    callback(handle_stop, Reason, StateName, State2).

-spec code_change(term() | {down, term()}, statename(), state(), term()) ->
                         {ok, statename(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec handle_pkt(pdu(), statename(), state()) -> {ok, statename(), state()} |
                                                 {error, error_reason(), state()}.
handle_pkt(#pdu{command_id = CmdID, sequence_number = Seq} = Pkt,
           StateName, #{seq := Seq} = State) when ?is_response(CmdID) ->
    %% TODO: check that the response type correlates with request type,
    %% because so far we don't check it elsewhere and assume it does
    case maps:take(in_flight, State) of
        {{_, _}, State1} when StateName == binding ->
            case handle_bind_resp(Pkt, State1) of
                {ok, bound, State2} ->
                    {ok, bound, dequeue_req(State2)};
                Other ->
                    Other
            end;
        {{Time, Sender}, State1} when StateName == bound ->
            State2 = reply(State1, {ok, {Pkt#pdu.command_status, Pkt#pdu.body}}, Time, Sender),
            {ok, StateName, dequeue_req(State2)};
        error ->
            report_unexpected_response(Pkt, StateName, State),
            {ok, StateName, State}
    end;
handle_pkt(#pdu{command_id = CmdID} = Pkt, StateName, State) when ?is_response(CmdID) ->
    report_unexpected_response(Pkt, StateName, State),
    {ok, StateName, State};
handle_pkt(Pkt, binding, State) ->
    case handle_bind_req(Pkt, State) of
        {ok, bound, State1} ->
            State2 = set_keepalive_timeout(State1, smsc),
            {ok, bound, dequeue_req(State2)};
        Other ->
            Other
    end;
handle_pkt(Pkt, bound, State) ->
    case handle_request(Pkt, State) of
        {ok, State1} ->
            State2 = set_keepalive_timeout(State1, smsc),
            {ok, bound, State2};
        {error, _, _} = Err ->
            Err
    end.

-spec handle_request(pdu(), state()) -> {ok, state()} | {error, error_reason(), state()}.
handle_request(#pdu{body = #unbind{}} = Pkt, State) ->
    State1 = send_resp(State, #unbind_resp{}, Pkt),
    {error, unbinded, State1};
handle_request(#pdu{body = #enquire_link{}} = Pkt, State) ->
    State1 = send_resp(State, #enquire_link_resp{}, Pkt),
    {ok, State1};
handle_request(#pdu{body = #alert_notification{} = Body}, State) ->
    ?LOG_WARNING("Got alert notification:~n~s", [pp(Body)]),
    {ok, State};
handle_request(#pdu{body = #deliver_sm{} = Body} = Pkt,
               #{role := esme, mode := Mode} = State) when ?is_receiver(Mode) ->
    {Status, Resp, State1} = callback(handle_deliver, Body, State),
    State2 = send_resp(State1, Resp, Pkt, Status),
    {ok, State2};
handle_request(#pdu{body = #submit_sm{} = Body} = Pkt,
               #{role := smsc, mode := Mode} = State) when ?is_receiver(Mode) ->
    {Status, Resp, State1} = callback(handle_submit, Body, State),
    State2 = send_resp(State1, Resp, Pkt, Status),
    {ok, State2};
handle_request(Pkt, State) ->
    report_unexpected_pkt(Pkt, bound, State),
    State1 = send_resp(State, #generic_nack{}, Pkt, ?ESME_RINVCMDID),
    {ok, State1}.

-spec reply(state(), send_reply(), millisecs(), sender()) -> state().
reply(State, _, _, undefined) ->
    State;
reply(State, Resp, Time, Sender) ->
    case current_time() >= Time of
        true -> State;
        false ->
            case Sender of
                Fun when is_function(Fun) ->
                    Fun(Resp, State);
                From ->
                    gen_statem:reply(From, Resp),
                    State
            end
    end.

%%%-------------------------------------------------------------------
%%% Binding
%%%-------------------------------------------------------------------
-spec send_bind_req(state()) -> state().
send_bind_req(#{role := esme, mode := Mode,
                system_id := SystemId, password := Password} = State) ->
    BindReq = case Mode of
                  transceiver ->
                      #bind_transceiver{
                         system_id = SystemId,
                         password = Password};
                  receiver ->
                      #bind_receiver{
                         system_id = SystemId,
                         password = Password};
                  transmitter ->
                      #bind_transmitter{
                         system_id = SystemId,
                         password = Password}
              end,
    send_req(State, BindReq).

-spec handle_bind_resp(pdu(), state()) -> {ok, statename(), state()} |
                                          {error, error_reason(), state()}.
handle_bind_resp(#pdu{command_status = Status}, State) ->
    case Status of
        ?ESME_ROK ->
            State1 = callback(handle_bound, State),
            {ok, bound, State1};
        _ ->
            {error, {bind_failed, Status}, State}
    end.

-spec handle_bind_req(pdu(), state()) -> {ok, statename(), state()}.
handle_bind_req(#pdu{body = #bind_transceiver{
                               system_id = SysId,
                               interface_version = Ver}} = Pkt,
            #{role := smsc} = State) ->
    BindResp = #bind_transceiver_resp{
                  system_id = SysId,
                  sc_interface_version = Ver},
    State1 = State#{system_id => SysId, mode => transceiver},
    State2 = send_resp(State1, BindResp, Pkt),
    State3 = callback(handle_bound, State2),
    {ok, bound, State3};
handle_bind_req(#pdu{body = #bind_receiver{
                               system_id = SysId,
                               interface_version = Ver}} = Pkt,
            #{role := smsc} = State) ->
    BindResp = #bind_receiver_resp{
                  system_id = SysId,
                  sc_interface_version = Ver},
    State1 = State#{system_id => SysId, mode => transmitter},
    State2 = send_resp(State1, BindResp, Pkt),
    State3 = callback(handle_bound, State2),
    {ok, bound, State3};
handle_bind_req(#pdu{body = #bind_transmitter{
                               system_id = SysId,
                               interface_version = Ver}} = Pkt,
            #{role := smsc} = State) ->
    BindResp = #bind_transmitter_resp{
                  system_id = SysId,
                  sc_interface_version = Ver},
    State1 = State#{system_id => SysId, mode => receiver},
    State2 = send_resp(State1, BindResp, Pkt),
    State3 = callback(handle_bound, State2),
    {ok, bound, State3};
handle_bind_req(Pkt, State) ->
    report_unexpected_pkt(Pkt, binding, State),
    State1 = send_resp(State, #generic_nack{}, Pkt, ?ESME_RINVBNDSTS),
    {ok, binding, State1}.

%%%-------------------------------------------------------------------
%%% Decoder
%%%-------------------------------------------------------------------
-spec decode(binary(), statename(), state()) ->
                    gen_statem:event_handler_result(statename()).
decode(Data, StateName, State) ->
    Buf = maps:get(buf, State, <<>>),
    do_decode(<<Buf/binary, Data/binary>>, StateName, State).

-spec do_decode(binary(), statename(), state()) ->
                       gen_statem:event_handler_result(statename()).
do_decode(<<Len:32, _/binary>>, StateName, State)
  when Len < 16 orelse Len >= ?MAX_BUF_SIZE ->
    reconnect({codec_failure, {bad_length, Len}}, StateName, State);
do_decode(<<Len:32, _/binary>> = Buf, StateName, State)
  when size(Buf) >= Len ->
    <<PDU:Len/binary, Buf1/binary>> = Buf,
    case smpp34pdu:unpack(PDU) of
        {ok, [#pdu{command_id = Id, body = {error, _}}], _} ->
            reconnect({codec_failure, {bad_body, Id}}, StateName, State);
        {ok, [#pdu{command_status = Status,
                   sequence_number = Seq,
                   body = Body} = Pkt], <<>>} ->
            ?LOG_DEBUG("Got PDU (status = ~p, seq = ~p):~n~s",
                       [Status, Seq, pp(Body)]),
            case handle_pkt(Pkt, StateName, State) of
                {ok, StateName1, State1} ->
                    do_decode(Buf1, StateName1, State1);
                {error, Reason, State1} ->
                    reconnect(Reason, StateName, State1)
            end;
        _ ->
            reconnect(codec_failure, StateName, State)
    end;
do_decode(Buf, StateName, State) ->
    sock_activate(State),
    next_state(StateName, State#{buf => Buf}).

%%%-------------------------------------------------------------------
%%% Send/enqueue/dequeue requests/responses
%%%-------------------------------------------------------------------
-spec send_req(state(), valid_pdu()) -> state().
send_req(State, Body) ->
    send_req(State, Body, undefined, undefined).

-spec send_req(state(), valid_pdu(), sender(), undefined | millisecs()) -> state().
send_req(State, Body, Sender, undefined) ->
    Timeout = maps:get(keepalive_timeout, State),
    send_req(State, Body, Sender, current_time() + Timeout);
send_req(#{in_flight := _} = State, Body, Sender, Time) ->
    enqueue_req(State, Body, Sender, Time);
send_req(State, Body, Sender, Time) ->
    Seq = (maps:get(seq, State, 0) rem 16#7FFFFFFF) + 1,
    State1 = State#{seq => Seq, in_flight => {Time, Sender}},
    send_pkt(State1, ?ESME_ROK, Seq, Body),
    State2 = unset_keepalive_timeout(State1, esme),
    set_request_timeout(State2).

-spec send_resp(state(), valid_pdu(), pdu()) -> state().
send_resp(State, Body, Req) ->
    send_resp(State, Body, Req, ?ESME_ROK).

-spec send_resp(state(), valid_pdu(), pdu(), non_neg_integer()) -> state().
send_resp(State, Body, #pdu{sequence_number = Seq}, Status) ->
    send_pkt(State, Status, Seq, Body),
    State.

-spec send_pkt(state(), non_neg_integer(), non_neg_integer(), valid_pdu()) -> ok.
send_pkt(State, Status, Seq, Body) ->
    Data = smpp34pdu:pack(Status, Seq, Body),
    ?LOG_DEBUG("Send PDU (status = ~p, seq = ~p):~n~s",
               [Status, Seq, pp(Body)]),
    sock_send(State, Data).

-spec enqueue_req(state(), valid_pdu(), sender(), millisecs()) -> state().
enqueue_req(State, Body, Sender, Time) ->
    State3 = case is_overloaded(State) of
                 false -> State;
                 true ->
                     State1 = report_overload(State),
                     State2 = drop_stale_requests(State1),
                     case is_overloaded(State2) of
                         true -> discard_all_requests(overloaded, State2);
                         false -> State2
                     end
             end,
    #{rq := RQ, rq_size := Size} = State3,
    RQ1 = queue:in({Time, Body, Sender}, RQ),
    State3#{rq => RQ1, rq_size => Size+1}.

-spec dequeue_req(state()) -> state().
dequeue_req(#{rq := RQ, rq_size := Size} = State) ->
    case queue:out(RQ) of
        {{value, {Time, Body, Sender}}, RQ1} ->
            State1 = State#{rq => RQ1, rq_size => Size-1},
            case current_time() >= Time of
                true -> dequeue_req(State1);
                false -> send_req(State1, Body, Sender, Time)
            end;
        {empty, _} ->
            State1 = unset_request_timeout(State),
            set_keepalive_timeout(State1, esme)
    end.

-spec discard_requests(error_reason(), state()) -> state().
discard_requests(Reason, #{role := Role} = State) ->
    State1 = discard_in_flight_request(Reason, State),
    case Role of
        smsc -> discard_all_requests(Reason, State1);
        esme -> drop_stale_requests(State1)
    end.

-spec discard_in_flight_request(error_reason(), state()) -> state().
discard_in_flight_request(Reason, State) ->
    case maps:take(in_flight, State) of
        {{Time, Sender}, State1} ->
            reply(State1, {error, Reason}, Time, Sender);
        error ->
            State
    end.

-spec discard_all_requests(error_reason(), state()) -> state().
discard_all_requests(Reason, #{rq := RQ} = State) ->
    State2 = lists:foldl(
               fun({Time, _, Sender}, State1) ->
                       reply(State1, {error, Reason}, Time, Sender)
               end, State, queue:to_list(RQ)),
    State2#{rq => queue:new(), rq_size => 0}.

-spec drop_stale_requests(state()) -> state().
drop_stale_requests(#{rq := RQ, rq_size := Size} = State) ->
    case queue:out(RQ) of
        {{value, {Time, _, _}}, RQ1} ->
            case current_time() >= Time of
                true ->
                    drop_stale_requests(
                      State#{rq => RQ1, rq_size => Size-1});
                false ->
                    State
            end;
        {empty, _} ->
            State
    end.

-spec is_overloaded(state()) -> boolean().
is_overloaded(#{rq_size := Size, rq_limit := Limit}) ->
    Size >= Limit.

%%%-------------------------------------------------------------------
%%% State transitions
%%% ACHTUNG: There is some fuckery with timeouts
%%%-------------------------------------------------------------------
-spec reconnect(error_reason(), statename(), state()) ->
                       gen_statem:event_handler_result(statename()).
reconnect(Reason, StateName, #{role := esme} = State) ->
    sock_close(State),
    State1 = discard_requests(Reason, State),
    State2 = callback(handle_disconnected, Reason, StateName, State1),
    State3 = maps:without([socket, transport, buf, seq,
                           keepalive_time, response_time],
                          State2),
    {next_state, connecting, State3, reconnect_timeout(State3)};
reconnect(Reason, StateName, #{role := smsc} = State) ->
    State1 = discard_requests(Reason, State),
    State2 = callback(handle_disconnected, Reason, StateName, State1),
    {stop, normal, State2}.

-spec bind(state()) -> gen_statem:event_handler_result(statename()).
bind(#{role := Role} = State) ->
    State1 = case Role of
                 esme -> send_bind_req(State);
                 smsc -> State
             end,
    State2 = maps:without([keepalive_time, response_time], State1),
    {next_state, binding, State2, bind_timeout(State2)}.

-spec next_state(statename(), state()) ->
                        gen_statem:event_handler_result(statename()).
next_state(StateName, #{keepalive_time := KeepAliveTime,
                        response_time := ResponseTime} = State) ->
    Time = min(KeepAliveTime, ResponseTime),
    Type = case Time of
               ResponseTime -> response;
               KeepAliveTime -> keepalive
           end,
    Timeout = max(0, Time - current_time()),
    {next_state, StateName, State, {timeout, Timeout, Type}};
next_state(StateName, #{keepalive_time := Time} = State) ->
    Timeout = max(0, Time - current_time()),
    {next_state, StateName, State, {timeout, Timeout, keepalive}};
next_state(StateName, #{response_time := Time} = State) ->
    Timeout = max(0, Time - current_time()),
    {next_state, StateName, State, {timeout, Timeout, response}};
next_state(StateName, State) ->
    {next_state, StateName, State}.

-spec current_time() -> millisecs().
current_time() ->
    erlang:system_time(millisecond).

-spec init_state(state(), peer_role()) -> state().
init_state(State, Role) ->
    State#{vsn => ?VSN, role => Role,
           rq => queue:new(), rq_size => 0}.

%%%-------------------------------------------------------------------
%%% Timers
%%%-------------------------------------------------------------------
-spec reconnect_timeout(state()) -> gen_statem:state_timeout().
reconnect_timeout(#{reconnect_timeout := Timeout}) ->
    ?LOG_DEBUG("Setting reconnect timeout to ~.3fs", [Timeout/1000]),
    {state_timeout, Timeout, reconnect}.

-spec bind_timeout(state()) -> gen_statem:state_timeout().
bind_timeout(#{bind_timeout := Timeout}) ->
    ?LOG_DEBUG("Setting bind timeout to ~.3fs", [Timeout/1000]),
    {state_timeout, Timeout, reconnect}.

-spec set_request_timeout(state()) -> state().
set_request_timeout(#{keepalive_timeout := Timeout} = State) ->
    ?LOG_DEBUG("Setting request timeout to ~.3fs", [Timeout/1000]),
    set_timeout(response_time, Timeout, State).

-spec unset_request_timeout(state()) -> state().
unset_request_timeout(State) ->
    ?LOG_DEBUG("Unsetting request timeout"),
    maps:remove(response_time, State).

-spec set_keepalive_timeout(state(), peer_role()) -> state().
set_keepalive_timeout(#{keepalive_timeout := Timeout, role := Role} = State, Role) ->
    ?LOG_DEBUG("Setting keepalive timeout to ~.3fs", [Timeout/1000]),
    set_timeout(keepalive_time, Timeout, State);
set_keepalive_timeout(State, _) ->
    State.

-spec unset_keepalive_timeout(state(), peer_role()) -> state().
unset_keepalive_timeout(#{role := Role} = State, Role) ->
    ?LOG_DEBUG("Unsetting keepalive timeout"),
    maps:remove(keepalive_time, State);
unset_keepalive_timeout(State, _) ->
    State.

-spec set_timeout(keepalive_time | response_time, millisecs(), state()) -> state().
set_timeout(Key, Timeout, State) ->
    State#{Key => current_time() + Timeout}.

%%%-------------------------------------------------------------------
%%% Socket management
%%%-------------------------------------------------------------------
-spec sock_connect(state()) -> {ok, state()} | {error, inet:posix()}.
sock_connect(#{host := Host, port := Port} = State) ->
    Timeout = maps:get(connect_timeout, State),
    ?LOG_DEBUG("Connecting to SMSC at ~ts:~B", [Host, Port]),
    case gen_tcp:connect(Host, Port,
                         [binary, {active, once}],
                         Timeout) of
        {ok, Sock} ->
            {ok, State#{socket => Sock, transport => gen_tcp}};
        {error, _} = Err ->
            Err
    end.

-spec sock_activate(state()) -> ok.
sock_activate(#{socket := Sock, transport := Transport}) ->
    Mod = case Transport of
              gen_tcp -> inet;
              _ -> Transport
          end,
    _ = Mod:setopts(Sock, [{active, once}]),
    ok.

-spec sock_close(state()) -> ok.
sock_close(#{socket := Sock, transport := Transport}) ->
    _ = Transport:close(Sock),
    ok;
sock_close(_State) ->
    ok.

-spec sock_send(state(), iodata()) -> ok.
sock_send(#{socket := Sock, transport := Transport}, Data) ->
    _ = Transport:send(Sock, Data),
    ok.

%%%-------------------------------------------------------------------
%%% Callbacks
%%%-------------------------------------------------------------------
-spec callback(atom(), state()) -> state().
callback(Fun, #{callback := Mod} = State) ->
    case erlang:function_exported(Mod, Fun, 1) of
        true -> Mod:Fun(State);
        false -> default_callback(Fun, State)
    end;
callback(Fun, State) ->
    default_callback(Fun, State).

-spec callback(atom(), valid_pdu(), state()) -> {non_neg_integer(), valid_pdu(), state()}.
callback(Fun, Body, #{callback := Mod} = State) ->
    case erlang:function_exported(Mod, Fun, 2) of
        false -> default_callback(Fun, Body, State);
        true ->
            case Mod:Fun(Body, State) of
                {Status, State1} -> {Status, default_response(Body), State1};
                Ret -> Ret
            end
    end;
callback(Fun, Body, State) ->
    default_callback(Fun, Body, State).

-spec callback(atom(), term(), statename(), state()) -> state().
callback(Fun, Term, StateName, #{callback := Mod} = State) ->
    case erlang:function_exported(Mod, Fun, 3) of
        true -> Mod:Fun(State);
        false -> default_callback(Fun, Term, StateName, State)
    end;
callback(Fun, Term, StateName, State) ->
    default_callback(Fun, Term, StateName, State).

-spec default_callback(atom(), state()) -> state().
default_callback(handle_bound, State) ->
    report_bound(State),
    State;
default_callback(_, State) ->
    State.

-spec default_callback(atom(), valid_pdu(), state()) -> {non_neg_integer(), valid_pdu(), state()}.
default_callback(_, Body, State) ->
    {?ESME_RINVCMDID, default_response(Body), State}.

-spec default_callback(atom(), term(), statename(), state()) -> state().
default_callback(handle_disconnected, Reason, StateName, State) ->
    report_disconnect(Reason, StateName, State),
    State;
default_callback(_, _, _, State) ->
    State.

-spec default_response(deliver_sm()) -> deliver_sm_resp();
                      (submit_sm()) -> submit_sm_resp().
default_response(#deliver_sm{}) -> #deliver_sm_resp{};
default_response(#submit_sm{}) -> #submit_sm_resp{}.

%%%-------------------------------------------------------------------
%%% Logging
%%%-------------------------------------------------------------------
-spec report_disconnect(error_reason(), statename(), state()) -> ok.
report_disconnect(Reason, StateName,
                  #{role := esme, host := Host, port := Port}) ->
    Fmt = case StateName of
              connecting -> "Failed to connect to SMSC at ~ts:~B: ~s";
              binding -> "Failed to bind to SMSC at ~ts:~B: ~s";
              bound -> "Connection with SMSC at ~ts:~B has failed: ~s"
          end,
    ?LOG_ERROR(Fmt, [Host, Port, format_error(Reason)]);
report_disconnect(timeout, binding, #{role := smsc, port := Port}) ->
    ?LOG_ERROR("ESME peer is idle in binding state: "
               "forcing disconnect from port ~B",
               [Port]);
report_disconnect(Reason, _StateName, #{role := smsc, port := Port}) ->
    ?LOG_ERROR("ESME peer has disconnected from port ~B: ~s",
               [Port, format_error(Reason)]).

-spec report_unexpected_pkt(pdu(), statename(), state()) -> ok.
report_unexpected_pkt(Pkt, StateName, State) ->
    ?LOG_WARNING("Unexpected PDU in state '~s':~n"
                 "** PDU:~n~s~n"
                 "** State: ~p",
                 [StateName, pp(Pkt), State]).

-spec report_unexpected_msg(gen_statem:event_type(), term(), statename(), state()) -> ok.
report_unexpected_msg(EventType, Msg, StateName, State) ->
    ?LOG_WARNING("Unexpected ~p in state '~s':~n"
                 "** Msg = ~p~n"
                 "** State = ~p",
                 [EventType, StateName, Msg, State]).

-spec report_unexpected_response(pdu(), statename(), state()) -> ok.
report_unexpected_response(Pkt, StateName, State) ->
    ?LOG_WARNING("Got unexpected response in state '~s':~n"
                 "** Pkt:~n~s~n"
                 "** State: ~p",
                 [StateName, pp(Pkt), State]).

-spec report_bound(state()) -> ok.
report_bound(#{role := smsc, mode := Mode, port := Port}) ->
    ?LOG_NOTICE("ESME peer has successfully bound to port ~B as ~s",
                [Port, reverse_mode(Mode)]);
report_bound(#{role := esme, mode := Mode, host := Host, port := Port}) ->
    ?LOG_NOTICE("Successfully bound to SMSC at ~ts:~B as ~s",
                [Host, Port, Mode]).

-spec report_overload(state()) -> state().
report_overload(State) ->
    Time = current_time(),
    LastTime = maps:get(last_overload_report, State, 0),
    case (Time - LastTime) >= timer:seconds(30) of
        true ->
            ?LOG_WARNING("Request queue is overfilled "
                         "(current size = ~B, limit = ~p)",
                         [maps:get(rq_size, State),
                          maps:get(rq_limit, State)]),
            State#{last_overload_report => Time};
        false ->
            State
    end.

-spec reverse_mode(mode()) -> mode().
reverse_mode(transceiver) -> transceiver;
reverse_mode(receiver) -> transmitter;
reverse_mode(transmitter) -> receiver.

%%%-------------------------------------------------------------------
%%% Pretty printer
%%%-------------------------------------------------------------------
pp(alert_notification, _) -> record_info(fields, alert_notification);
pp(bind_receiver, _) -> record_info(fields, bind_receiver);
pp(bind_receiver_resp, _) -> record_info(fields, bind_receiver_resp);
pp(bind_transceiver, _) -> record_info(fields, bind_transceiver);
pp(bind_transceiver_resp, _) -> record_info(fields, bind_transceiver_resp);
pp(bind_transmitter, _) -> record_info(fields, bind_transmitter);
pp(bind_transmitter_resp, _) -> record_info(fields, bind_transmitter_resp);
pp(cancel_sm, _) -> record_info(fields, cancel_sm);
pp(cancel_sm_resp, _) -> record_info(fields, cancel_sm_resp);
pp(data_sm, _) -> record_info(fields, data_sm);
pp(data_sm_resp, _) -> record_info(fields, data_sm_resp);
pp(deliver_sm, _) -> record_info(fields, deliver_sm);
pp(deliver_sm_resp, _) -> record_info(fields, deliver_sm_resp);
pp(enquire_link, _) -> record_info(fields, enquire_link);
pp(enquire_link_resp, _) -> record_info(fields, enquire_link_resp);
pp(generic_nack, _) -> record_info(fields, generic_nack);
pp(outbind, _) -> record_info(fields, outbind);
pp(pdu, _) -> record_info(fields, pdu);
pp(query_sm, _) -> record_info(fields, query_sm);
pp(query_sm_resp, _) -> record_info(fields, query_sm_resp);
pp(replace_sm, _) -> record_info(fields, replace_sm);
pp(replace_sm_resp, _) -> record_info(fields, replace_sm_resp);
pp(submit_sm, _) -> record_info(fields, submit_sm);
pp(submit_sm_resp, _) -> record_info(fields, submit_sm_resp);
pp(unbind, _) -> record_info(fields, unbind);
pp(unbind_resp, _) -> record_info(fields, unbind_resp);
pp(_, _) -> no.
