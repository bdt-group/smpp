-module(black_hole_smsc).

-include_lib("smpp34pdu/include/smpp34pdu.hrl").

-behaviour(gen_smsc).

-export([start/2, stop/1, handle_submit/2, handle_data/2]).

start(Id, Opts) ->
    gen_smsc:start(Id, ?MODULE, Opts#{id => Id}).

stop(Ref) ->
    gen_smsc:stop(Ref).

handle_submit(_SubmitSm, State) ->
    swallow(),
    {?ESME_ROK, State}.

handle_data(_DataSm, State) ->
    swallow(),
    {?ESME_ROK, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
swallow() ->
    receive
    after infinity -> ok
    end.