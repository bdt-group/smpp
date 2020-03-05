%%%-------------------------------------------------------------------
%%% @author xram <xram@xram>
%%% @copyright (C) 2020, xram
%%% @doc
%%%
%%% @end
%%% Created :  3 Mar 2020 by xram <xram@xram>
%%%-------------------------------------------------------------------
-module(gen_esme).

%% API
-export([start/2, start/3]).
-export([start_link/2, start_link/3]).
-export([child_spec/3]).

-include("smpp.hrl").

-define(BIND_TIMEOUT, timer:seconds(10)).
-define(CONNECT_TIMEOUT, timer:seconds(5)).
-define(RECONNECT_TIMEOUT, timer:seconds(1)).
-define(KEEPALIVE_TIMEOUT, timer:seconds(10)).
-define(REQUEST_QUEUE_LIMIT, 1000000).

-type state() :: smpp_socket:state().
-type error_reason() :: smpp_socket:error_reason().
-type statename() :: smpp_socket:statename().
-type socket_name() :: smpp_socket:socket_name().
-export_type([state/0, error_reason/0, statename/0]).

-callback handle_connected(state()) -> state().
-callback handle_disconnected(error_reason(), statename(), state()) -> state().
-callback handle_bound(state()) -> state().
-callback handle_deliver(deliver_sm(), state()) -> {non_neg_integer(), state()}.
-callback handle_stop(term(), statename(), state()) -> any().

%% All callbacks are optional
-optional_callbacks([handle_connected/1,
                     handle_disconnected/3,
                     handle_bound/1,
                     handle_deliver/2,
                     handle_stop/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec child_spec(socket_name(), module() | undefined, map()) ->
                        supervisor:child_spec().
child_spec(Name, Mod, Opts) ->
    State = opts_to_state(Mod, Opts),
    smpp_socket:child_spec(esme, Name, get_id_by_name(Name), State).

-spec start(module() | undefined, map()) -> {ok, pid()} | {error, term()}.
start(Mod, Opts) ->
    smpp_socket:connect(opts_to_state(Mod, Opts)).

-spec start(socket_name(), module() | undefined, map()) ->
                   {ok, pid()} | {error, term()}.
start(Name, Mod, Opts) ->
    smpp_socket:connect(Name, opts_to_state(Mod, Opts)).

-spec start_link(module() | undefined, map()) -> {ok, pid()} | {error, term()}.
start_link(Mod, Opts) ->
    smpp_socket:connect_link(opts_to_state(Mod, Opts)).

-spec start_link(socket_name(), module() | undefined, map()) ->
                        {ok, pid()} | {error, term()}.
start_link(Name, Mod, Opts) ->
    smpp_socket:connect_link(Name, opts_to_state(Mod, Opts)).

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_id_by_name({local, Name}) -> Name;
get_id_by_name({via, _, Name}) -> Name;
get_id_by_name({global, Name}) -> Name.

opts_to_state(Mod, Opts) ->
    Host = maps:get(host, Opts, "localhost"),
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    SystemId = maps:get(system_id, Opts, ""),
    Password = maps:get(password, Opts, ""),
    Mode = maps:get(mode, Opts, transceiver),
    RQLimit = maps:get(request_queue_limit, Opts, ?REQUEST_QUEUE_LIMIT),
    ConnectTimeout = maps:get(connect_timeout, Opts, ?CONNECT_TIMEOUT),
    ReconnectTimeout = maps:get(reconnect_timeout, Opts, ?RECONNECT_TIMEOUT),
    KeepAliveTimeout = maps:get(keepalive_timeout, Opts, ?KEEPALIVE_TIMEOUT),
    BindTimeout = maps:get(bind_timeout, Opts, ?BIND_TIMEOUT),
    State = Opts#{host => Host,
                  port => Port,
                  mode => Mode,
                  system_id => SystemId,
                  password => Password,
                  rq_limit => RQLimit,
                  connect_timeout => ConnectTimeout,
                  reconnect_timeout => ReconnectTimeout,
                  bind_timeout => BindTimeout,
                  keepalive_timeout => KeepAliveTimeout},
    case Mod of
        undefined ->
            State;
        _ ->
            {module, Mod} = code:ensure_loaded(Mod),
            State#{callback => Mod}
    end.
