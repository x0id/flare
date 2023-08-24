-module(flare).
-include("flare_internal.hrl").

-compile(inline).
-compile({inline_size, 512}).

-export([
    async_produce/6,
    produce/6,
    receive_response/2
]).

%% public
-spec async_produce(topic(), timestamp(), key(), value(), headers(),
    pid() | undefined) -> {ok, req_id()} | {error, atom()}.

async_produce(Topic, Timestamp, Key, Value, Headers, Pid) ->
    case flare_topic:server(Topic) of
        {ok, {_BufferSize, Server}} ->
            {Msg, Size} = flare_utils:msg(Timestamp, Key, Value, Headers),

            Sema = persistent_term:get({flare_sema, Server}),
            case sema_nif:occupy(Sema, Size) of
                {ok, _} ->
                    Self = self(),
                    ReleaseFun = fun (Cnt) -> sema_nif:vacate(Sema, Cnt, Self) end,
                    ReqId = {os:timestamp(), Self},
                    Server ! {produce, ReqId, Msg, Size, Pid, ReleaseFun},
                    {ok, ReqId};
                {error, backlog_full} ->
                    error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec produce(topic(), timestamp(), key(), value(), headers(),
    timeout()) -> {ok, req_id()} | {error, atom()}.

produce(Topic, Timestamp, Key, Value, Headers, Timeout) ->
    case async_produce(Topic, Timestamp, Key, Value, Headers, self()) of
        {ok, ReqId} ->
            receive_response(ReqId, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

-spec receive_response(req_id(), timeout()) ->
    ok | {error, atom()}.

receive_response(ReqId, Timeout) ->
    receive
        {ReqId, Response} ->
            Response
    after Timeout ->
        {error, timeout}
    end.
