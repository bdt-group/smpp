%%%-------------------------------------------------------------------
%%% @author xram <xram@xram>
%%% @copyright (C) 2021, Big Data Technology. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%% @doc
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
-export([stop/1]).
-export([format_error/1]).
-export([pp/1]).
-export([get_id_by_name/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([connecting/3, binding/3, bound/3,
         rate_limit_cooldown/3, in_flight_limit_cooldown/3]).

-include("smpp.hrl").
-include_lib("kernel/include/logger.hrl").

-define(VSN, 1).
-define(MAX_BUF_SIZE, 104857600).
-define(is_request(CmdID), ((CmdID band 16#80000000) == 0)).
-define(is_response(CmdID), ((CmdID band 16#80000000) /= 0)).
-define(is_receiver(Mode), (Mode == receiver orelse Mode == transceiver)).

-type state() :: #{vsn := non_neg_integer(),
                   id := term(),
                   role := peer_role(),
                   reconnect := boolean(),
                   proxy := boolean(),
                   in_flight := [{seq(), in_flight_request()}],
                   in_flight_limit := pos_integer(),
                   req_per_sec := undefined | pos_integer(),
                   max_await_reqs := undefined | pos_integer(),
                   await_reqs_counter := counters:counters_ref(),
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
                   buf => binary(),
                   seq => 1..16#7FFFFFFF,
                   socket => port(),
                   transport => module(),
                   current_rps => {millisecs(), non_neg_integer()},
                   _ => term()}.

-type seq() :: non_neg_integer().
-type statename() :: connecting | binding | bound |
                     rate_limit_cooldown | in_flight_limit_cooldown.
-type peer_role() :: smsc | esme.
-type mode() :: transmitter | receiver | transceiver.
-type millisecs() :: pos_integer().
-type codec_error_reason() :: {bad_length, non_neg_integer()} |
                              {bad_body, non_neg_integer()}.
% Any of those reasons are possible and client should be able
% to handle all of them regardless.
% Relative frequency of occurence of each one of them depends on
% SMSC implementation and one's timings.
-type flow_control_reason() :: sending_expired | rate_limit | in_flight_limit.
-type error_reason() :: closed | timeout | unbinded |
                        system_shutdown | system_error |
                        {inet, inet:posix()} |
                        {bind_failed, pos_integer()} |
                        codec_failure |
                        {codec_failure, codec_error_reason()} |
                        flow_control_reason().
-type socket_name() :: {global, term()} | {via, module(), term()} | {local, atom()}.
-type sender() :: {pid(), term()} | send_callback() | undefined.
-type in_flight_request() :: {ReqDeadline :: millisecs(), RespDeadline :: millisecs(), sender()}.
-type send_reply() :: {ok, {non_neg_integer(), valid_pdu()}} |
                      {error, error_reason() | overload}.
-type send_callback() :: fun((send_reply() | {error, flow_control_reason()}, state()) -> state()).


-export_type([error_reason/0, socket_name/0]).
-export_type([send_reply/0, send_callback/0]).
-export_type([statename/0, state/0]).

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

-spec stop(gen_statem:server_ref()) -> ok;
          (state()) -> state().
stop(#{} = State) ->
    %% We're fooling dialyzer here
    try erlang:nif_error(normal) of
        _ -> State
    catch _:_ ->
            put(shutdown_state, State),
            exit(normal)
    end;
stop(Ref) ->
    gen_statem:cast(Ref, stop).

-spec send_async(gen_statem:server_ref(), valid_pdu()) -> ok | {error, overload}.
send_async(Ref, Pkt) ->
    send_async(Ref, Pkt, undefined).

-spec send_async(gen_statem:server_ref(), valid_pdu(), undefined | send_callback()
) -> ok | {error, overload}.
send_async(Ref, Pkt, Fun) ->
    case enqueue_req(Ref) of
        ok ->
            gen_statem:cast(Ref, {send_req, Pkt, Fun, undefined});
        error ->
            {error, overload}
    end.

-spec send_async(gen_statem:server_ref(), valid_pdu(), undefined | send_callback(), millisecs()
) -> ok | {error, overload}.
send_async(Ref, Pkt, Fun, Timeout) ->
    case enqueue_req(Ref, Timeout) of
        ok when is_integer(Timeout) ->
            Time = current_time() + Timeout,
            gen_statem:cast(Ref, {send_req, Pkt, Fun, Time});
        ok ->
            gen_statem:cast(Ref, {send_req, Pkt, Fun, undefined});
        error ->
            {error, overload}
    end.

-spec send(gen_statem:server_ref(), valid_pdu(), millisecs()) -> send_reply().
send(Ref, Pkt, Timeout) ->
    Time = current_time() + Timeout,
    case enqueue_req(Ref, Timeout) of
        ok ->
            try gen_statem:call(Ref, {send_req, Pkt, Time}, {dirty_timeout, Timeout})
            catch exit:{timeout, {gen_statem, call, _}} ->
                    {error, timeout};
                  exit:{_, {gen_statem, call, _}} ->
                    {error, closed}
            end;
        error ->
            {error, overload}
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
format_error(sending_expired) ->
    "stale request";
format_error(rate_limit) ->
    "discarded by rate limit";
format_error(in_flight_limit) ->
    "threshold amount met for in flight requests";
format_error({bind_failed, Status}) ->
    "binding failed (status = " ++ integer_to_list(Status) ++ ")".

-spec pp(any()) -> iolist().
pp(Term) ->
    io_lib_pretty:print(Term, fun pp/2).

-spec get_id_by_name({local, T} | {via, module(), T} | {global, T}) -> T.
get_id_by_name({local, Name}) -> Name;
get_id_by_name({via, _, Name}) -> Name;
get_id_by_name({global, Name}) -> Name.

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
                                    max_await_reqs => undefined,
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
    State1 = init_rate_limit(State),
    State2 = init_req_counter(State1),
    {ok, connecting, State2}.

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
connecting({call, _From}, {send_req, _Body, _Time}, State) ->
    keep_state(State, [postpone]);
connecting(cast, {send_req, _Body, _Fun, _Time}, State) ->
    keep_state(State, [postpone]);
connecting(cast, stop, State) ->
    {stop, normal, State};
connecting(EventType, Msg, State) ->
    State1 = callback(handle_event, EventType, Msg, ?FUNCTION_NAME, State),
    keep_state(State1).

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
binding({call, _From}, {send_req, _Body, _Time}, State) ->
    keep_state(State, [postpone]);
binding(cast, {send_req, _Body, _Fun, _Time}, State) ->
    keep_state(State, [postpone]);
binding(cast, stop, State) ->
    {stop, normal, State};
binding(EventType, Msg, State) ->
    State1 = callback(handle_event, EventType, Msg, ?FUNCTION_NAME, State),
    keep_state(State1, [postpone]).

-spec bound(gen_statem:event_type(), term(), state()) ->
                       gen_statem:event_handler_result(statename()).
bound(info, {tcp, Sock, Data}, #{socket := Sock} = State) ->
    decode(Data, ?FUNCTION_NAME, State);
bound(info, {tcp_closed, Sock}, #{socket := Sock} = State) ->
    reconnect(closed, ?FUNCTION_NAME, State);
bound(info, {tcp_error, Sock, Reason}, #{socket := Sock} = State) ->
    reconnect({inet, Reason}, ?FUNCTION_NAME, State);
bound(timeout, keepalive, State) ->
    State1 = send_req(State, #enquire_link{}),
    keep_state(State1);
bound(timeout, TimeoutType, State) ->
    ?LOG_DEBUG("Timeout type:~p", [TimeoutType]),
    reconnect(timeout, ?FUNCTION_NAME, State);
bound({call, From}, {send_req, Body, Time}, State) ->
    % send request operation is the same for call and cast methods,
    % fun send_req/4 is polymorphic on 3rd argument for pid/tag and callback fun
    bound(cast, {send_req, Body, From, Time}, State);
bound(cast, {send_req, Body, Sender, undefined}, State) ->
    case check_limits(State) of
        {ok, State1} ->
            ok = dequeue_reg(State),
            State2 = send_req(State1, Body, Sender, undefined),
            keep_state(State2);
        {error, {NextState, State1, Actions}} ->
            next_state(NextState, State1, Actions)
    end;
bound(cast, {send_req, Body, Sender, Time}, State) ->
    case current_time() > Time of
        true ->
            ok = dequeue_reg(State),
            State1 = reply(State, {error, sending_expired}, current_time(), Sender),
            keep_state(State1, []);
        false ->
            case check_limits(State) of
                {ok, State1} ->
                    ok = dequeue_reg(State),
                    State2 = send_req(State1, Body, Sender, Time),
                    keep_state(State2);
                {error, {NextState, State1, Actions}} ->
                    next_state(NextState, State1, Actions)
            end
    end;
bound(cast, stop, State) ->
    {stop, normal, State};
bound(EventType, Msg, State) ->
    State1 = callback(handle_event, EventType, Msg, ?FUNCTION_NAME, State),
    keep_state(State1, []).

-spec rate_limit_cooldown(gen_statem:event_type(), term(), state()) ->
          gen_statem:event_handler_result(statename()).
rate_limit_cooldown(info, {tcp, Sock, Data}, #{socket := Sock} = State) ->
    decode(Data, ?FUNCTION_NAME, State);
rate_limit_cooldown(timeout, keepalive, #{role := esme} = State) ->
    keep_state(State, [{next_event, timeout, keepalive}]);
rate_limit_cooldown(state_timeout, rate_limit_reset, State) ->
    next_state(bound, State);
rate_limit_cooldown({call, From}, {send_req, Body, Time}, State) ->
    rate_limit_cooldown(cast, {send_req, Body, From, Time}, State);
rate_limit_cooldown(cast, {send_req, _Body, _Sender, undefined}, State) ->
    keep_state(State, [postpone]);
rate_limit_cooldown(cast, {send_req, _Body, Sender, Time}, State) ->
    case current_time() > Time of
        true ->
            ok = dequeue_reg(State),
            State1 = reply(State, {error, rate_limit}, current_time(), Sender),
            keep_state(State1);
        false ->
            keep_state(State, [postpone])
    end;
rate_limit_cooldown(EventType, EventContent, State) ->
    bound(EventType, EventContent, State).

-spec in_flight_limit_cooldown(gen_statem:event_type(), term(), state()) ->
          gen_statem:event_handler_result(statename()).
in_flight_limit_cooldown(info, {tcp, Sock, Data}, #{socket := Sock} = State) ->
    decode(Data, ?FUNCTION_NAME, State);
in_flight_limit_cooldown(timeout, keepalive, #{role := esme} = State) ->
    keep_state(State, [{next_event, timeout, keepalive}]);
in_flight_limit_cooldown({call, From}, {send_req, Body, Time}, State) ->
    in_flight_limit_cooldown(cast, {send_req, Body, From, Time}, State);
in_flight_limit_cooldown(cast, {send_req, _Body, _Sender, undefined}, State) ->
    keep_state(State, [postpone]);
in_flight_limit_cooldown(cast, {send_req, _Body, Sender, Time}, State) ->
    case current_time() > Time of
        true ->
            ok = dequeue_reg(State),
            State1 = reply(State, {error, in_flight_limit}, current_time(), Sender),
            keep_state(State1);
        false ->
            keep_state(State, [postpone])
    end;
in_flight_limit_cooldown(EventType, EventContent, State) ->
    bound(EventType, EventContent, State).

-spec terminate(term(), statename(), state()) -> any().
terminate(Why, StateName, State0) ->
    ?LOG_DEBUG("Terminating, reason:~p, state:~p, statedata:~p", [Why, StateName, State0]),
    State = case erase(shutdown_state) of
                undefined -> State0;
                St -> St
            end,
    Reason = case Why of
                 normal -> closed;
                 shutdown -> system_shutdown;
                 {shutdown, _} -> system_shutdown;
                 _ -> system_error
             end,
    State1 = discard_in_flight_requests(Reason, State),
    callback(handle_stop, Reason, StateName, State1).

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
           StateName, #{seq := CurSeq, in_flight:= InFlight} = State) when
      ?is_response(CmdID) andalso Seq =< CurSeq ->
    %% TODO: check that the response type correlates with request type,
    %% because so far we don't check it elsewhere and assume it does
    case lists:keytake(Seq, 1, InFlight) of
        {value, _, InFlight1} when StateName == binding ->
            case handle_bind_resp(Pkt, State#{in_flight => InFlight1}) of
                {ok, bound, State1} ->
                    State2 = set_keepalive_timeout(State1, esme),
                    {ok, bound, State2};
                Other ->
                    Other
            end;
        {value, {Seq, {Time, _, Sender}}, InFlight1}
          when StateName == bound;
               StateName == in_flight_limit_cooldown;
               StateName == rate_limit_cooldown ->
            State1 = reply(State#{in_flight => InFlight1},
                           {ok, {Pkt#pdu.command_status, Pkt#pdu.body}}, Time, Sender),
            NextState = case StateName of
                            rate_limit_cooldown -> StateName;
                            _ -> bound
                        end,
            State2 = case State1 of
                #{in_flight := []} -> maps:without([response_time], State1);
                #{in_flight := _} -> set_request_timeout(State1)
            end,
            {ok, NextState, State2};
        false ->
            report_unexpected_response(Pkt, StateName, State),
            State2 = set_keepalive_timeout(State, esme),
            {ok, StateName, State2}
    end;
handle_pkt(#pdu{command_id = CmdID} = Pkt, StateName, State) when ?is_response(CmdID) ->
    report_unexpected_response(Pkt, StateName, State),
    {ok, StateName, State};
handle_pkt(Pkt, binding, State) ->
    case handle_bind_req(Pkt, State) of
        {ok, bound, State1} ->
            State2 = set_keepalive_timeout(State1, smsc),
            {ok, bound, State2};
        Other ->
            Other
    end;
handle_pkt(Pkt, StateName, State) when StateName == bound;
                                       StateName == rate_limit_cooldown;
                                       StateName == in_flight_limit_cooldown ->
    case handle_request(Pkt, StateName, State) of
        {ok, State1} ->
            State2 = set_keepalive_timeout(State1, smsc),
            {ok, StateName, State2};
        {error, _, _} = Err ->
            Err
    end.

-spec handle_request(pdu(), statename(), state()) -> {ok, state()} | {error, error_reason(), state()}.
handle_request(#pdu{body = #unbind{}} = Pkt, _, State) ->
    State1 = send_resp(State, #unbind_resp{}, Pkt),
    {error, unbinded, State1};
handle_request(#pdu{body = #enquire_link{}} = Pkt, _, State) ->
    State1 = send_resp(State, #enquire_link_resp{}, Pkt),
    {ok, State1};
handle_request(#pdu{body = #alert_notification{} = Body}, _, State) ->
    ?LOG_WARNING("Got alert notification:~n~s", [pp(Body)]),
    {ok, State};
handle_request(#pdu{body = #deliver_sm{} = Body} = Pkt, _,
               #{role := Role, mode := Mode, proxy := Proxy} = State)
  when ?is_receiver(Mode) andalso (Role == esme orelse Proxy == true) ->
    {Status, Resp, State1} = callback(handle_deliver, Body, State),
    State2 = send_resp(State1, Resp, Pkt, Status),
    {ok, State2};
handle_request(#pdu{body = #submit_sm{} = Body} = Pkt, _,
               #{role := Role, mode := Mode, proxy := Proxy} = State)
  when ?is_receiver(Mode) andalso (Role == smsc orelse Proxy == true) ->
    {Status, Resp, State1} = callback(handle_submit, Body, State),
    State2 = send_resp(State1, Resp, Pkt, Status),
    {ok, State2};
handle_request(Pkt, StateName, State) ->
    report_unexpected_pkt(Pkt, StateName, State),
    State1 = send_resp(State, #generic_nack{}, Pkt, ?ESME_RINVCMDID),
    {ok, State1}.

-spec reply(state(), send_reply() | {error, stale}, millisecs(), sender()) -> state().
reply(State, _, _, undefined) ->
    State;
reply(State, Resp, Time, Sender) ->
    Now = current_time(),
    case Sender of
        Fun when is_function(Fun) ->
            Fun(Resp, State);
        From when Now < Time ->
            gen_statem:reply(From, Resp),
            State;
        _ ->
            State
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
    process_result(State, Status).

-spec handle_bind_req(pdu(), state()) -> {ok, statename(), state()} |
                                         {error, error_reason(), state()}.
handle_bind_req(#pdu{body = Bind} = Pkt,
            #{role := smsc} = State) ->
    {Status, BindResp, State1} = callback(handle_bind, Bind, State),
    State2 = send_resp(State1, BindResp, Pkt, Status),
    process_result(State2, Status);
handle_bind_req(Pkt, State) ->
    report_unexpected_pkt(Pkt, binding, State),
    State1 = send_resp(State, #generic_nack{}, Pkt, ?ESME_RINVBNDSTS),
    {ok, binding, State1}.

-spec process_result(state(), non_neg_integer()) -> {ok, statename(), state()} |
                                                    {error, error_reason(), state()}.
process_result(State, Status) ->
    case Status of
        ?ESME_ROK ->
            State1 = callback(handle_bound, State),
            {ok, bound, State1};
        _ ->
            {error, {bind_failed, Status}, State}
    end.

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
%%% Send requests/responses
%%%-------------------------------------------------------------------
-spec send_req(state(), valid_pdu()) -> state().
send_req(State, Body) ->
    send_req(State, Body, undefined, undefined).

-spec send_req(state(), valid_pdu(), sender(), undefined | millisecs()) -> state().
send_req(State, Body, Sender, undefined) ->
    Timeout = maps:get(keepalive_timeout, State),
    send_req(State, Body, Sender, current_time() + Timeout);
send_req(#{in_flight := InFlight} = State, Body, Sender, Time) ->
    Seq = (maps:get(seq, State, 0) rem 16#7FFFFFFF) + 1,
    RespDeadline = current_time() + maps:get(keepalive_timeout, State),
    State1 = State#{seq => Seq, in_flight => [{Seq, {Time, RespDeadline, Sender}} | InFlight]},
    case Body of
        #submit_sm{} ->
            _ = callback(handle_sending, Body, State),
            ok;
        _ ->
            ok
    end,
    send_pkt(State1, ?ESME_ROK, Seq, Body),
    State2 = set_keepalive_timeout(State1, esme),
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

-spec discard_in_flight_requests(error_reason(), state()) -> state().
discard_in_flight_requests(Reason, #{in_flight := InFlight} = State) ->
    lists:foldr(fun({_Seq, {Time, _, Sender}}, State1) ->
                        reply(State1, {error, Reason}, Time, Sender)
                end, State#{in_flight => []}, InFlight).

%%%-------------------------------------------------------------------
%%% State transitions
%%% ACHTUNG: There is some fuckery with timeouts
%%%-------------------------------------------------------------------
-spec reconnect(error_reason(), statename(), state()) ->
                       gen_statem:event_handler_result(statename()).
reconnect(Reason, StateName, #{reconnect := true} = State) ->
    sock_close(State),
    State1 = discard_in_flight_requests(Reason, State),
    State2 = callback(handle_disconnected, Reason, StateName, State1),
    State3 = maps:without([socket, transport, buf, seq,
                           keepalive_time, response_time],
                          State2),
    {next_state, connecting, State3, reconnect_timeout(State3)};
reconnect(Reason, StateName, #{reconnect := false} = State) ->
    State1 = discard_in_flight_requests(Reason, State),
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

-spec keep_state(state()) -> gen_statem:event_handler_result(statename()).
keep_state(State) ->
    keep_state(State, []).

-spec keep_state(state(), [gen_statem:action()]) ->
          gen_statem:event_handler_result(statename()).
keep_state(State, Actions) ->
    {keep_state, State, set_timeouts(State) ++ Actions}.

-spec next_state(statename(), state()) ->
                        gen_statem:event_handler_result(statename()).
next_state(StateName, State) ->
    next_state(StateName, State, []).

-spec next_state(statename(), state(), [gen_statem:action()]) ->
                        gen_statem:event_handler_result(statename()).
next_state(StateName, State, Actions) ->
    {next_state, StateName, State, set_timeouts(State) ++ Actions}.

-spec set_timeouts(state()) -> [gen_statem:action()].
set_timeouts(#{keepalive_time := KeepAliveTime,
               response_time := ResponseTime}) ->
    Time = min(KeepAliveTime, ResponseTime),
    Type = case Time of
               ResponseTime -> response;
               KeepAliveTime -> keepalive
           end,
    Timeout = max(0, Time - current_time()),
    [{timeout, Timeout, Type}];
set_timeouts(#{keepalive_time := Time}) ->
    Timeout = max(0, Time - current_time()),
    [{timeout, Timeout, keepalive}];
set_timeouts(#{response_time := Time}) ->
    Timeout = max(0, Time - current_time()),
    [{timeout, Timeout, response}];
set_timeouts(_) ->
    [].

-spec current_time() -> millisecs().
current_time() ->
    erlang:system_time(millisecond).

-spec init_state(state(), peer_role()) -> state().
init_state(State, Role) ->
    State#{vsn => ?VSN, role => Role,
           in_flight => []}.

%%%-------------------------------------------------------------------
%%% Timers
%%%-------------------------------------------------------------------
-spec reconnect_timeout(state()) -> {state_timeout, millisecs(), reconnect}.
reconnect_timeout(#{reconnect_timeout := Timeout}) ->
    ?LOG_DEBUG("Setting reconnect timeout to ~.3fs", [Timeout/1000]),
    {state_timeout, Timeout, reconnect}.

-spec bind_timeout(state()) -> {state_timeout, millisecs(), reconnect}.
bind_timeout(#{bind_timeout := Timeout}) ->
    ?LOG_DEBUG("Setting bind timeout to ~.3fs", [Timeout/1000]),
    {state_timeout, Timeout, reconnect}.

-spec set_request_timeout(state()) -> state().
set_request_timeout(#{in_flight := InFlight} = State) ->
    {_, {_, RespDeadline, _}} = lists:last(InFlight),
    ?LOG_DEBUG("Setting request timeout to ~.3fs", [(RespDeadline - current_time())/1000]),
    State#{response_time => RespDeadline}.

-spec set_keepalive_timeout(state(), peer_role()) -> state().
set_keepalive_timeout(#{keepalive_timeout := Timeout, role := Role} = State, Role) ->
    ?LOG_DEBUG("Setting keepalive timeout to ~.3fs", [Timeout/1000]),
    State#{keepalive_time => current_time() + Timeout};
set_keepalive_timeout(State, Role) ->
    ?LOG_DEBUG("Role mismatch, state:~p role: ~p", [State, Role]),
    State.

-spec unset_keepalive_timeout(state(), peer_role()) -> state().
unset_keepalive_timeout(#{role := Role} = State, Role) ->
    ?LOG_DEBUG("Unsetting keepalive timeout"),
    maps:remove(keepalive_time, State).

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
        false -> default_callback(Fun, State);
        true ->
            case Mod:Fun(State) of
                ignore -> default_callback(Fun, State);
                State1 -> State1
            end
    end;
callback(Fun, State) ->
    default_callback(Fun, State).

-spec callback(atom(), valid_pdu(), state()) -> {non_neg_integer(), valid_pdu(), state()}.
callback(Fun, Body, #{callback := Mod} = State) ->
    case erlang:function_exported(Mod, Fun, 2) of
        false -> default_callback(Fun, Body, State);
        true ->
            case Mod:Fun(Body, State) of
                ignore -> default_callback(Fun, Body, State);
                {Status, State1} -> {Status, default_response(Body), State1};
                Ret -> Ret
            end
    end;
callback(Fun, Body, State) ->
    default_callback(Fun, Body, State).

-spec callback(atom(), term(), statename(), state()) -> state().
callback(Fun, Term, StateName, #{callback := Mod} = State) ->
    case erlang:function_exported(Mod, Fun, 3) of
        false -> default_callback(Fun, Term, StateName, State);
        true ->
            case Mod:Fun(Term, StateName, State) of
                ignore -> default_callback(Fun, Term, StateName, State);
                State1 -> State1
            end
    end;
callback(Fun, Term, StateName, State) ->
    default_callback(Fun, Term, StateName, State).

-spec callback(atom(), term(), term(), statename(), state()) -> state().
callback(Fun, EventType, Msg, StateName, #{callback := Mod} = State) ->
    case erlang:function_exported(Mod, Fun, 4) of
        false -> default_callback(Fun, EventType, Msg, StateName, State);
        true ->
            case Mod:Fun(EventType, Msg, StateName, State) of
                ignore -> default_callback(Fun, EventType, Msg, StateName, State);
                State1 -> State1
            end
    end;
callback(Fun, EventType, Msg, StateName, State) ->
    default_callback(Fun, EventType, Msg, StateName, State).

-spec default_callback(atom(), state()) -> state().
default_callback(handle_bound, State) ->
    report_bound(State),
    State;
default_callback(_, State) ->
    State.

-spec default_callback(atom(), valid_pdu(), state()) -> {non_neg_integer(), valid_pdu(), state()}.
default_callback(handle_bind, Body, State) ->
    default_bind(Body, State);
default_callback(_, Body, State) ->
    {?ESME_RINVCMDID, default_response(Body), State}.

-spec default_callback(atom(), term(), statename(), state()) -> state().
default_callback(handle_disconnected, Reason, StateName, State) ->
    report_disconnect(Reason, StateName, State),
    State;
default_callback(_, _, _, State) ->
    State.

-spec default_callback(atom(), gen_statem:event_type(), term(), statename(), state()) -> state().
default_callback(handle_event, EventType, Msg, StateName, State) ->
    report_unexpected_msg(EventType, Msg, StateName, State),
    State.

-spec default_response(deliver_sm()) -> deliver_sm_resp();
                      (submit_sm()) -> submit_sm_resp().
default_response(#deliver_sm{}) -> #deliver_sm_resp{};
default_response(#submit_sm{}) -> #submit_sm_resp{}.


-spec default_bind(pdu(), state()) ->
    {pos_integer(),
        bind_receiver_resp() |
        bind_transceiver_resp() |
        bind_transmitter_resp() |
        generic_nack(), state()}.
default_bind(#pdu{body = #bind_transceiver{
                               system_id = SysId,
                               interface_version = Ver}},
            #{role := smsc} = State) ->
    BindResp = #bind_transceiver_resp{
                  system_id = SysId,
                  sc_interface_version = Ver},
    State1 = State#{system_id => SysId, mode => transmitter},
    {?ESME_ROK, BindResp, State1};
default_bind(#pdu{body = #bind_receiver{
                               system_id = SysId,
                               interface_version = Ver}},
            #{role := smsc} = State) ->
    BindResp = #bind_receiver_resp{
                  system_id = SysId,
                  sc_interface_version = Ver},
    State1 = State#{system_id => SysId, mode => transmitter},
    {?ESME_ROK, BindResp, State1};
default_bind(#pdu{body = #bind_transmitter{
                               system_id = SysId,
                               interface_version = Ver}},
            #{role := smsc} = State) ->
    BindResp = #bind_transmitter_resp{
                  system_id = SysId,
                  sc_interface_version = Ver},
    State1 = State#{system_id => SysId, mode => receiver},
    {?ESME_ROK, BindResp, State1};
default_bind(_, State) ->
    {?ESME_RINVBNDSTS, #generic_nack{}, State}.

%%%-------------------------------------------------------------------
%%% Logging
%%%-------------------------------------------------------------------
-spec report_disconnect(error_reason(), statename(), state()) -> ok.
report_disconnect(Reason, StateName,
                  #{role := esme, host := Host, port := Port}) ->
    Fmt = case StateName of
              connecting ->
                  "Failed to connect to SMSC at ~ts:~B: ~s";
              binding ->
                  "Failed to bind to SMSC at ~ts:~B: ~s";
              _ when StateName == bound;
                     StateName == in_flight_limit_cooldown;
                     StateName == rate_limit_cooldown ->
                  "Connection with SMSC at ~ts:~B has failed: ~s"
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

%%%-------------------------------------------------------------------
%%% Overload protection
%%%-------------------------------------------------------------------
-define(PT_MAX_RPS(Id), {?MODULE, Id, max_rps}).
-spec init_rate_limit(state()) -> state().
init_rate_limit(#{req_per_sec := undefined} = State) ->
    State;
init_rate_limit(#{req_per_sec := Rps, id := Id} = State) when is_integer(Rps) ->
    ok = persistent_term:put(?PT_MAX_RPS(Id), Rps),
    State#{current_rps => {erlang:monotonic_time(second), 0}}.


-spec check_limits(state()) ->
          {ok, state()} |
          {error, {statename(), state(), [gen_statem:action()]}}.
check_limits(#{req_per_sec := undefined} = State) ->
    check_in_flight(State);
check_limits(#{req_per_sec := MaxRps, current_rps := {CurSecond, CurRate}} = State) ->
    case erlang:monotonic_time(second) of
        CurSecond when CurRate == MaxRps ->
            Now = erlang:monotonic_time(millisecond),
            % max here to ensure non negative timeout
            Timeout = max(0, (CurSecond + 1) * 1000 - Now),
            {error, {rate_limit_cooldown,
                     State,
                     [postpone,
                      {state_timeout, Timeout, rate_limit_reset}]}};
        CurSecond ->
            check_in_flight(State#{current_rps => {CurSecond, CurRate + 1}});
        NewSecond ->
            check_in_flight(State#{current_rps => {NewSecond, 1}})
    end.

-spec check_in_flight(state()) ->
          {ok, state()} |
          {error, {statename(), state(), [gen_statem:action()]}}.
check_in_flight(#{in_flight := InFlight, in_flight_limit := Limit} = State)
  when length(InFlight) =< Limit ->
    {ok, State};
check_in_flight(State) ->
    {error, {in_flight_limit_cooldown, State, [postpone]}}.

%%%-------------------------------------------------------------------
%%% ESME back pressure
%%%-------------------------------------------------------------------
-define(PT_MAX_AWAIT_REQS(Id), {?MODULE, Id, max_await_reqs}).
-define(PT_AWAIT_REQS_COUNTER(Id), {?MODULE, Id, await_reqs_counter}).

-spec init_req_counter(state()) -> state().
init_req_counter(#{max_await_reqs := undefined} = State) ->
    State;
init_req_counter(#{max_await_reqs := MaxAwaitReqs, id := Id} = State) ->
    %% Back pressure here does not rely on exact counter value by design.
    %% I.e. a bit more than MaxAwaitReqs can be queued due to a natural race
    %% but then back pressure will kick in anyway. That is why 'write_concurrency'
    %% counters option fits well here.
    CounterRef = counters:new(1, [write_concurrency]),
    ok = persistent_term:put(?PT_AWAIT_REQS_COUNTER(Id), CounterRef),
    ok = persistent_term:put(?PT_MAX_AWAIT_REQS(Id), MaxAwaitReqs),
    State#{await_reqs_counter => CounterRef}.

-spec enqueue_req(gen_statem:server_ref()) -> ok | error.
enqueue_req(Ref) ->
    enqueue_req(Ref, undefined).

-spec enqueue_req(gen_statem:server_ref(), millisecs() | undefined) -> ok | error.
enqueue_req(Pid, _Timeout) when is_pid(Pid) ->
    %% TODO: add back pressure support for direct Pid invocations.
    ok;
enqueue_req(Name, Timeout) ->
    Id = get_id_by_name(Name),
    case persistent_term:get(?PT_MAX_AWAIT_REQS(Id), undefined) of
        undefined ->
            ok;
        MaxAwaitReqs ->
            CounterRef = persistent_term:get(?PT_AWAIT_REQS_COUNTER(Id)),
            case counters:get(CounterRef, 1) of
                Counter when Counter >= MaxAwaitReqs ->
                    error;
                Counter when Timeout /= undefined ->
                    case is_timeout_feasible(Timeout, Counter, Id) of
                        true ->
                            ok = counters:add(CounterRef, 1, 1);
                        false ->
                            error
                    end;
                _ ->
                    ok = counters:add(CounterRef, 1, 1)
            end
    end.

-spec is_timeout_feasible(millisecs(), non_neg_integer(), term()) -> boolean().
is_timeout_feasible(Timeout, Counter, Id) ->
    case persistent_term:get(?PT_MAX_RPS(Id), undefined) of
        undefined ->
            true;
        Rps ->
            TimeoutSec = Timeout div 1000,
            QueuedSec = Counter / Rps,
            TimeoutSec > QueuedSec
    end.

-spec dequeue_reg(state()) -> ok.
dequeue_reg(#{max_await_reqs := undefined}) ->
    ok;
dequeue_reg(#{await_reqs_counter := CounterRef}) ->
    ok = counters:sub(CounterRef, 1, 1).
