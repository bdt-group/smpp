-module(faulty_smsc_1).

-include_lib("smpp34pdu/include/smpp34pdu.hrl").

-behaviour(gen_smsc).

-export([start/2, handle_bind/2]).

%%%===================================================================
%%% API
%%%===================================================================
start(Id, Opts) ->
    gen_smsc:start(Id, ?MODULE, Opts#{id => Id}).

handle_bind(#bind_transceiver{system_id = SysId, interface_version = Ver}, State) ->
    Resp = #bind_transceiver_resp{system_id = SysId, sc_interface_version = Ver},
    {?ESME_RBINDFAIL, Resp, State}.
