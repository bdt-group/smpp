%%%-------------------------------------------------------------------
%%% @author xram <xram@xram>
%%% @copyright (C) 2020, xram
%%% @doc
%%%
%%% @end
%%% Created :  3 Mar 2020 by xram <xram@xram>
%%%-------------------------------------------------------------------
-module(gen_smsc).

%% API
-export([start/3]).
-export([child_spec/3]).

-include("smpp.hrl").

-define(BIND_TIMEOUT, timer:seconds(5)).
-define(KEEPALIVE_TIMEOUT, timer:seconds(60)).
-define(REQUEST_QUEUE_LIMIT, 1000000).

-type state() :: smpp_socket:state().
-type error_reason() :: smpp_socket:error_reason().
-type statename() :: binding | connected.

-export_type([state/0, error_reason/0, statename/0]).

-callback handle_connected(state()) -> state().
-callback handle_disconnected(error_reason(), statename(), state()) -> state().
-callback handle_bound(state()) -> state().
-callback handle_submit(deliver_sm(), state()) -> {non_neg_integer(), state()}.
-callback handle_stop(term(), statename(), state()) -> any().

%% All callbacks are optional
-optional_callbacks([handle_connected/1,
                     handle_disconnected/3,
                     handle_bound/1,
                     handle_submit/2,
                     handle_stop/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec child_spec(term(), module() | undefined, map()) -> supervisor:child_spec().
child_spec(Id, Mod, Opts) ->
    {State, RanchOpts} = opts_to_state(Mod, Opts),
    smpp_socket:child_spec(smsc, Id, State, RanchOpts).

-spec start(term(), module() | undefined, map()) -> {ok, pid()} | {error, term()}.
start(Id, Mod, Opts) ->
    {State, RanchOpts} = opts_to_state(Mod, Opts),
    smpp_socket:listen(Id, State, RanchOpts).

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec opts_to_state(module() | undefined, map()) -> {state(), ranch_tcp:opts()}.
opts_to_state(Mod, Opts) ->
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    RQLimit = maps:get(request_queue_limit, Opts, ?REQUEST_QUEUE_LIMIT),
    KeepAliveTimeout = maps:get(keepalive_timeout, Opts, ?KEEPALIVE_TIMEOUT),
    BindTimeout = maps:get(bind_timeout, Opts, ?BIND_TIMEOUT),
    RanchOpts = [{port, Port}],
    State = Opts#{rq_limit => RQLimit,
                  bind_timeout => BindTimeout,
                  keepalive_timeout => KeepAliveTimeout},
    case Mod of
        undefined ->
            {State, RanchOpts};
        _ ->
            {module, Mod} = code:ensure_loaded(Mod),
            {State#{callback => Mod}, RanchOpts}
    end.
