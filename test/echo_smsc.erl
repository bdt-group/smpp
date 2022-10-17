-module(echo_smsc).

-include_lib("smpp34pdu/include/smpp34pdu.hrl").

-behaviour(gen_smsc).

-export([start/2, stop/1, handle_submit/2]).

%%%===================================================================
%%% API
%%%===================================================================
start(Id, Opts) ->
    gen_smsc:start(Id, ?MODULE, Opts#{id => Id}).

stop(Ref) ->
    gen_smsc:stop(Ref).

handle_submit(SubmitSm, State) ->
    ok = gen_smsc:send_async(self(), echo_submit_sm(SubmitSm)),
    {?ESME_ROK, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
echo_submit_sm(#submit_sm{source_addr_ton = DestAddrTon,
                          source_addr_npi = DestAddrNpi,
                          source_addr = DestAddr,
                          dest_addr_ton = SrcAddrTon,
                          dest_addr_npi = SrcAddrNpi,
                          destination_addr = SrcAddr,
                          short_message = ShortMessage}) ->
    #deliver_sm{source_addr_ton = SrcAddrTon,
                source_addr_npi = SrcAddrNpi,
                source_addr = SrcAddr,
                dest_addr_ton = DestAddrTon,
                dest_addr_npi = DestAddrNpi,
                destination_addr = DestAddr,
                short_message = ShortMessage}.
