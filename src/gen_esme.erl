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
-module(gen_esme).

%% API
-export([start/2, start/3]).
-export([stop/1]).
-export([start_link/2, start_link/3]).
-export([send/3]).
-export([send_async/2, send_async/3, send_async/4]).
-export([call/3, reply/2]).
-export([format_error/1]).
-export([pp/1]).
-export([child_spec/3]).

-include("smpp.hrl").

-define(BIND_TIMEOUT, timer:seconds(10)).
-define(CONNECT_TIMEOUT, timer:seconds(5)).
-define(RECONNECT_TIMEOUT, timer:seconds(1)).
-define(KEEPALIVE_TIMEOUT, timer:seconds(10)).
-define(REQUEST_QUEUE_LIMIT, 1000000).
-define(IN_FLIGHT_LIMIT, 1).

-type state() :: smpp_socket:state().
-type error_reason() :: smpp_socket:error_reason().
-type statename() :: smpp_socket:statename().
-type socket_name() :: smpp_socket:socket_name().
-type send_callback() :: smpp_socket:send_callback().
-type send_reply() :: smpp_socket:send_reply().
-type event_type() :: gen_statem:event_type().
-type from() :: {pid(), term()}. %% gen_statem:from()

-export_type([state/0, error_reason/0, statename/0, send_reply/0, send_callback/0]).
-export_type([event_type/0, from/0]).

-callback handle_connected(state()) -> state() | ignore.
-callback handle_disconnected(error_reason(), statename(), state()) -> state() | ignore.
-callback handle_bound(state()) -> state() | ignore.
-callback handle_sending(submit_sm(), state()) -> any().
-callback handle_deliver(deliver_sm(), state()) -> {non_neg_integer(), state()} |
                                                   {non_neg_integer(), deliver_sm_resp(), state()} |
                                                   ignore.
-callback handle_submit(submit_sm(), state()) -> {non_neg_integer(), state()} |
                                                 {non_neg_integer(), submit_sm_resp(), state()} |
                                                 ignore.
-callback handle_stop(term(), statename(), state()) -> any().
-callback handle_event(event_type(), term(), statename(), state()) -> state() | ignore.

%% All callbacks are optional
-optional_callbacks([handle_connected/1,
                     handle_disconnected/3,
                     handle_bound/1,
                     handle_sending/2,
                     handle_deliver/2,
                     handle_submit/2,
                     handle_event/4,
                     handle_stop/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec child_spec(socket_name(), module() | undefined, map()) ->
                        supervisor:child_spec().
child_spec(Name, Mod, Opts) ->
    State = opts_to_state(Mod, Opts, Name),
    smpp_socket:child_spec(esme, smpp_socket:get_id_by_name(Name), State, Name).

-spec start(module() | undefined, map()) -> {ok, pid()} | {error, term()}.
start(Mod, Opts = #{id := _}) ->
    smpp_socket:connect(opts_to_state(Mod, Opts)).

-spec start(socket_name(), module() | undefined, map()) ->
                   {ok, pid()} | {error, term()}.
start(Name, Mod, Opts) ->
    smpp_socket:connect(Name, opts_to_state(Mod, Opts, Name)).

-spec stop(gen_statem:server_ref() | state()) -> any().
stop(Ref) ->
    smpp_socket:stop(Ref).

-spec start_link(module() | undefined, map()) -> {ok, pid()} | {error, term()}.
start_link(Mod, Opts = #{id := _}) ->
    smpp_socket:connect_link(opts_to_state(Mod, Opts)).

-spec start_link(socket_name(), module() | undefined, map()) ->
                        {ok, pid()} | {error, term()}.
start_link(Name, Mod, Opts) ->
    smpp_socket:connect_link(Name, opts_to_state(Mod, Opts, Name)).

-spec send(gen_statem:server_ref(), valid_pdu(), pos_integer()) -> send_reply().
send(Ref, Pkt, Timeout) ->
    smpp_socket:send(Ref, Pkt, Timeout).

-spec send_async(gen_statem:server_ref(), valid_pdu()) -> ok | {error, overload}.
send_async(Ref, Pkt) ->
    smpp_socket:send_async(Ref, Pkt).

-spec send_async(gen_statem:server_ref(), valid_pdu(),
                 undefined | send_callback()) -> ok | {error, overload}.
send_async(Ref, Pkt, Fun) ->
    smpp_socket:send_async(Ref, Pkt, Fun).

-spec send_async(gen_statem:server_ref(), valid_pdu(),
                 undefined | send_callback(), pos_integer()) -> ok | {error, overload}.
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
opts_to_state(Mod, Opts, Name) ->
    opts_to_state(Mod, Opts#{id => smpp_socket:get_id_by_name(Name)}).

opts_to_state(Mod, Opts) ->
    Host = maps:get(host, Opts, "localhost"),
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    SystemId = maps:get(system_id, Opts, ""),
    SystemType = maps:get(system_type, Opts, ""),
    InterfaceVersion = maps:get(interface_version, Opts, ?VERSION),
    Password = maps:get(password, Opts, ""),
    Mode = maps:get(mode, Opts, transceiver),
    Reconnect = maps:get(reconnect, Opts, true),
    IsProxy = maps:get(proxy, Opts, false),
    RQLimit = maps:get(request_queue_limit, Opts, ?REQUEST_QUEUE_LIMIT),
    InFlightLimit = maps:get(in_flight_limit, Opts, ?IN_FLIGHT_LIMIT),
    ConnectTimeout = maps:get(connect_timeout, Opts, ?CONNECT_TIMEOUT),
    ReconnectTimeout = maps:get(reconnect_timeout, Opts, ?RECONNECT_TIMEOUT),
    KeepAliveTimeout = maps:get(keepalive_timeout, Opts, ?KEEPALIVE_TIMEOUT),
    ResponseTimeout = maps:get(response_timeout, Opts, KeepAliveTimeout),
    BindTimeout = maps:get(bind_timeout, Opts, ?BIND_TIMEOUT),
    ReqPerSec = maps:get(req_per_sec, Opts, undefined),
    MaxAwaitReqs = maps:get(max_await_reqs, Opts, undefined),
    State = Opts#{host => Host,
                  port => Port,
                  mode => Mode,
                  system_id => SystemId,
                  system_type => SystemType,
                  interface_version => InterfaceVersion,
                  password => Password,
                  rq_limit => RQLimit,
                  in_flight_limit => InFlightLimit,
                  proxy => IsProxy,
                  reconnect => Reconnect,
                  connect_timeout => ConnectTimeout,
                  reconnect_timeout => ReconnectTimeout,
                  bind_timeout => BindTimeout,
                  req_per_sec => ReqPerSec,
                  max_await_reqs => MaxAwaitReqs,
                  keepalive_timeout => KeepAliveTimeout,
                  response_timeout => ResponseTimeout},
    case Mod of
        undefined ->
            State;
        _ ->
            case code:ensure_loaded(Mod) of
                {module, Mod} ->
                    State#{callback => Mod};
                Err ->
                    erlang:error({module_not_found, Mod, Err})
            end
    end.
