-module(test_esme_1).

-include_lib("smpp34pdu/include/smpp34pdu.hrl").

-behaviour(gen_esme).

-export([start/2, stop/1, handle_deliver/2, handle_data/2]).

%%%===================================================================
%%% API
%%%===================================================================
start(Name, Opts) ->
    gen_esme:start(Name, ?MODULE, Opts).

stop(Ref) ->
    gen_esme:stop(Ref).

handle_deliver(DeliverSm, State = #{subscriber := Pid}) ->
    Pid ! DeliverSm,
    {?ESME_ROK, State}.

handle_data(DataSm, State = #{subscriber := Pid}) ->
    ct:pal("rec data_sm: ~p", [DataSm]),
    Pid ! DataSm,
    {?ESME_ROK, #data_sm_resp{}, State}.