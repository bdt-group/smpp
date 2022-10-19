-module(faulty_echo_smsc).

-include_lib("smpp34pdu/include/smpp34pdu.hrl").

-behaviour(gen_smsc).

-export([start/2, handle_bind/2]).

%%%===================================================================
%%% API
%%%===================================================================
start(Id, Opts) ->
    gen_smsc:start(Id, ?MODULE, Opts#{id => Id}).

handle_bind(#bind_transceiver{system_id = SysId, interface_version = Ver},
            State = #{counter := Counter}) ->
    ok = counters:add(Counter, 1, 1),
    Resp = #bind_transceiver_resp{system_id = SysId, sc_interface_version = Ver},
    case counters:get(Counter, 1) rem 3 of
        0 ->
            {?ESME_ROK, Resp, State#{mode => transceiver}};
        _Other ->
            {?ESME_RBINDFAIL, Resp, State}
    end.
