%%%-------------------------------------------------------------------
%%% @author xram <xram@xram>
%%% @doc
%%%
%%% @end
%%% Created :  3 Mar 2020 by xram <xram@xram>
%%%-------------------------------------------------------------------
-module(gen_smsc).

%% API
-export([start/3]).
-export([send/3]).
-export([send_async/2, send_async/3, send_async/4]).
-export([format_error/1]).
-export([pp/1]).
-export([child_spec/3]).

-include("smpp.hrl").

-define(BIND_TIMEOUT, timer:seconds(5)).
-define(KEEPALIVE_TIMEOUT, timer:seconds(60)).
-define(REQUEST_QUEUE_LIMIT, 1000000).

-type state() :: smpp_socket:state().
-type error_reason() :: smpp_socket:error_reason().
-type statename() :: binding | connected.
-type send_callback() :: smpp_socket:send_callback().
-type send_reply() :: smpp_socket:send_reply().

-export_type([state/0, error_reason/0, statename/0, send_reply/0, send_callback/0]).

-callback handle_connected(state()) -> state().
-callback handle_disconnected(error_reason(), statename(), state()) -> state().
-callback handle_bound(state()) -> state().
-callback handle_submit(submit_sm(), state()) -> {non_neg_integer(), state()} |
                                                 {non_neg_integer(), submit_sm_resp(), state()}.
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

-spec format_error(error_reason()) -> string().
format_error(Reason) ->
    smpp_socket:format_error(Reason).

-spec pp(term()) -> iolist().
pp(Term) ->
    smpp_socket:pp(Term).

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
            case code:ensure_loaded(Mod) of
                {module, Mod} ->
                    {State#{callback => Mod}, RanchOpts};
                Err ->
                    erlang:error({module_not_found, Mod, Err})
            end
    end.
