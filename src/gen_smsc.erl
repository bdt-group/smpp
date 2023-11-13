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
%%% Created :  3 Mar 2020 by xram <xram@xram>
%%%-------------------------------------------------------------------
-module(gen_smsc).

%% API
-export([start/3]).
-export([stop/1]).
-export([send/3]).
-export([send_async/2, send_async/3, send_async/4]).
-export([call/3, reply/2]).
-export([format_error/1]).
-export([pp/1]).
-export([child_spec/3]).

-include("smpp.hrl").

-define(BIND_TIMEOUT, timer:seconds(5)).
-define(KEEPALIVE_TIMEOUT, timer:seconds(60)).
-define(REQUEST_QUEUE_LIMIT, 1000000).

-type state() :: smpp_socket:state().
-type error_reason() :: smpp_socket:error_reason().
-type statename() :: smpp_socket:statename().
-type send_callback() :: smpp_socket:send_callback().
-type send_reply() :: smpp_socket:send_reply().
-type event_type() :: gen_statem:event_type().
-type from() :: {pid(), term()}. %% gen_statem:from()
-type bind_pdu() :: bind_receiver() | bind_transceiver() | bind_transmitter().
-type bind_resp() :: bind_receiver_resp() | bind_transceiver_resp() | bind_transmitter_resp().

-export_type([state/0, error_reason/0, statename/0, send_reply/0, send_callback/0]).
-export_type([event_type/0, from/0]).

-callback handle_connected(state()) -> state() | ignore.
-callback handle_disconnected(error_reason(), statename(), state()) -> state() | ignore.
-callback handle_bind(bind_pdu(), state()) -> {non_neg_integer(), state()} |
                                     {non_neg_integer(), bind_resp(), state()} |
                                     ignore.
-callback handle_bound(state()) -> state() | ignore.
-callback handle_deliver(deliver_sm(), state()) -> {non_neg_integer(), state()} |
                                                   {non_neg_integer(), deliver_sm_resp(), state()} |
                                                   ignore.
-callback handle_submit(submit_sm(), state()) -> {non_neg_integer(), state()} |
                                                 {non_neg_integer(), submit_sm_resp(), state()} |
                                                 ignore.
-callback handle_data(data_sm(), state()) -> {non_neg_integer(), state()} |
                                             {non_neg_integer(), data_sm_resp(), state()} |
                                             ignore.
-callback handle_cancel(cancel_sm(), state()) -> {non_neg_integer(), state()} |
                                                 {non_neg_integer(), cancel_sm_resp(), state()} |
                                                 ignore.
-callback handle_stop(term(), statename(), state()) -> any().
-callback handle_event(event_type(), term(), statename(), state()) -> state() | ignore.

%% All callbacks are optional
-optional_callbacks([handle_connected/1,
                     handle_disconnected/3,
                     handle_bind/2,
                     handle_bound/1,
                     handle_deliver/2,
                     handle_submit/2,
                     handle_cancel/2,
                     handle_event/4,
                     handle_stop/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec child_spec(term(), module() | undefined, map()) -> supervisor:child_spec().
child_spec(Id, Mod, Opts) ->
    {State, RanchOpts} = opts_to_state(Mod, Opts, Id),
    smpp_socket:child_spec(smsc, Id, State, RanchOpts).

-spec start(term(), module() | undefined, map()) -> {ok, pid()} | {error, term()}.
start(Id, Mod, Opts) ->
    {State, RanchOpts} = opts_to_state(Mod, Opts, Id),
    smpp_socket:listen(Id, State, RanchOpts).

-spec stop(gen_statem:server_ref() | state()) -> any().
stop(Ref) ->
    smpp_socket:stop(Ref).

-spec send(gen_statem:server_ref(), valid_pdu(), pos_integer()) -> send_reply().
send(Ref, Pkt, Timeout) ->
    smpp_socket:send(Ref, Pkt, Timeout).

-spec send_async(gen_statem:server_ref(), valid_pdu()) -> ok.
send_async(Ref, Pkt) ->
    smpp_socket:send_async(Ref, Pkt).

-spec send_async(gen_statem:server_ref(), valid_pdu(),
                 undefined | send_callback()) -> ok.
send_async(Ref, Pkt, Fun) ->
    smpp_socket:send_async(Ref, Pkt, Fun).

-spec send_async(gen_statem:server_ref(), valid_pdu(),
                 undefined | send_callback(), pos_integer()) -> ok.
send_async(Ref, Pkt, Fun, Timeout) ->
    smpp_socket:send_async(Ref, Pkt, Fun, Timeout).

-spec call(gen_statem:server_ref(), term(), timeout()) -> term().
call(Ref, Term, Timeout) ->
    try gen_statem:call(Ref, Term, {dirty_timeout, Timeout})
    catch exit:{timeout, {gen_statem, call, _}} ->
            exit({timeout, {?MODULE, call, [Ref, Term, Timeout]}});
          exit:{_, {gen_statem, call, _}} ->
            exit({closed, {?MODULE, call, [Ref, Term, Timeout]}})
    end.

-spec reply(from(), term()) -> ok.
reply(From, Term) ->
    gen_statem:reply(From, Term).

-spec format_error(error_reason()) -> string().
format_error(Reason) ->
    smpp_socket:format_error(Reason).

-spec pp(term()) -> iolist().
pp(Term) ->
    smpp_socket:pp(Term).

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec opts_to_state(module() | undefined, map(), term()) -> {state(), ranch_tcp:opts()}.
opts_to_state(Mod, Opts, Id) ->
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    IP = maps:get(ip, Opts, ?DEFAULT_IP),
    RQLimit = maps:get(request_queue_limit, Opts, ?REQUEST_QUEUE_LIMIT),
    KeepAliveTimeout = maps:get(keepalive_timeout, Opts, ?KEEPALIVE_TIMEOUT),
    ResponseTimeout = maps:get(response_timeout, Opts, KeepAliveTimeout),
    BindTimeout = maps:get(bind_timeout, Opts, ?BIND_TIMEOUT),
    IsProxy = maps:get(proxy, Opts, false),
    RanchOpts = [{ip, IP}, {port, Port}],
    State = Opts#{id => Id,
                  rq_limit => RQLimit,
                  reconnect => false,
                  proxy => IsProxy,
                  bind_timeout => BindTimeout,
                  keepalive_timeout => KeepAliveTimeout,
                  response_timeout => ResponseTimeout},
    case Mod of
        undefined ->
            {State, RanchOpts};
        _ ->
            case code:ensure_loaded(Mod) of
                {module, Mod} ->
                    {State#{callback => Mod}, RanchOpts};
                Err ->
                    erlang:error({module_not_found, Mod, Err})
            end
    end.
