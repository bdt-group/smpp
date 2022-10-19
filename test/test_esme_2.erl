-module(test_esme_2).

-behaviour(gen_esme).

-export([start/2, handle_disconnected/3]).

%%%===================================================================
%%% API
%%%===================================================================
start(Name, Opts) ->
    gen_esme:start(Name, ?MODULE, Opts).

handle_disconnected(Reason, StateName, State = #{subscriber := Pid}) ->
    Pid ! {disconnected, Reason, StateName},
    State.
