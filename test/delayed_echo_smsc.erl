-module(delayed_echo_smsc).

-include_lib("smpp34pdu/include/smpp34pdu.hrl").

-behaviour(gen_smsc).

-export([start/2, stop/1, handle_submit/2]).

start(Id, Opts) ->
    gen_smsc:start(Id, ?MODULE, Opts#{id => Id}).

stop(Ref) ->
    gen_smsc:stop(Ref).

handle_submit(_SubmitSm, #{delay := Delay} = State) when is_integer(Delay), Delay > 0 ->
    timer:sleep(Delay),
    {?ESME_ROK, State};
handle_submit(_SubmitSm, State) ->
    {?ESME_ROK, State}.