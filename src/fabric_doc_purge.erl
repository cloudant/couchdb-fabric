% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric_doc_purge).

-export([go/3]).

-include_lib("fabric/include/fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

go(_, [], _) ->
    {ok, []};
go(DbName, AllIdsRevs, Opts) ->
    {TAllIds, TAllIdsRevs, LengthAllIdsRevs} = tag_docs(AllIdsRevs),
    Options = lists:delete(all_or_nothing, Opts),
    % Counters -> [{Worker, [{Tag, Id}]}]
    {Counters, Workers} = dict:fold(fun(Shard, TIdsRevs, {Cs,Ws}) ->
        {TIds, IdsRevs} = lists:unzip(TIdsRevs),
        #shard{name=Name, node=Node} = Shard,
        Ref = rexi:cast(Node, {fabric_rpc, purge_docs, [Name,IdsRevs,Options]}),
        Worker = Shard#shard{ref=Ref},
        { [{Worker, TIds}|Cs], [Worker|Ws]}
    end, {[], []}, group_idrevs_by_shard(DbName, TAllIdsRevs)),

    RexiMon = fabric_util:create_monitors(Workers),
    W = couch_util:get_value(w, Options, integer_to_list(mem3:quorum(DbName))),
    % ResultsAcc -> {PSeqsCs, DocsDict}
    % PSeqsCs -> [{Shard, PurgeSeq}]
    % DocsDict -> {Tag, Id} -> [{ok, PurgedRevs}]
    ResultsAcc = {[], dict:new()},
    Acc = {length(Workers), LengthAllIdsRevs, list_to_integer(W), Counters, ResultsAcc},
    Timeout = fabric_util:request_timeout(),
    try rexi_utils:recv(Workers, #shard.ref, fun handle_message/3, Acc, infinity, Timeout) of
    {ok, {Health, PSeq, Results}} when Health =:= ok; Health =:= accepted ->
        % Results: [{{Tag, Id}, {ok, Revs}}]
        {Health, {PSeq, [R || R <-
            couch_util:reorder_results(TAllIds, Results), R =/= noreply]}};
    {timeout, Acc1} ->
        {_, _, W1, Counters1, {PSeqsCs, DocsDict1}} = Acc1,
        {DefunctWorkers, _} = lists:unzip(Counters1),
        fabric_util:log_timeout(DefunctWorkers, "purge_docs"),
        {Health, _, Resp} = dict:fold(fun force_reply/3, {ok, W1, []}, DocsDict1),
        FinalPSeq = fabric_view_changes:pack_seqs(PSeqsCs),
        {Health, {FinalPSeq, [R || R <-
            couch_util:reorder_results(TAllIds, Resp), R =/= noreply]}};
    Else ->
        Else
    after
        rexi_monitor:stop(RexiMon)
    end.

handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Worker, Acc0) ->
    {_, LenDocs, W, Counters, {PSeqsCs, DocsDict0}} = Acc0,
    {FailCounters, NewCounters} = lists:partition(fun({#shard{node=N}, _}) ->
        N == NodeRef
    end, Counters),
    % fill DocsDict with error messages for relevant Docs
    DocsDict = lists:foldl(fun({_W, Docs}, CDocsDict) ->
        Replies = [{error, internal_server_error} || _D <- Docs],
        append_update_replies(Docs, Replies, CDocsDict)
    end, DocsDict0, FailCounters),
    Results = {PSeqsCs, DocsDict},
    skip_message({length(NewCounters), LenDocs, W, NewCounters, Results});
handle_message({rexi_EXIT, _}, Worker, Acc0) ->
    {WC, LenDocs , W, Counters, {PSeqsCs, DocsDict0}} = Acc0,
    % fill DocsDict with error messages for relevant Docs
    {value, {_W, Docs}, NewCounters} = lists:keytake(Worker, 1, Counters),
    Replies = [{error, internal_server_error} || _D <- Docs],
    DocsDict = append_update_replies(Docs, Replies, DocsDict0),
    skip_message({WC-1, LenDocs, W, NewCounters, {PSeqsCs, DocsDict}});
handle_message({ok, {PSeq, Replies0}}, Worker, Acc0) ->
    {WCount, DocCount, W, Counters, {PSeqsCs0, DocsDict0}} = Acc0,
    {value, {_W, Docs}, NewCounters} = lists:keytake(Worker, 1, Counters),
    DocsDict = append_update_replies(Docs, Replies0, DocsDict0),
    PSeqsCs = [{Worker, PSeq}| PSeqsCs0],
    case {WCount, dict:size(DocsDict)} of
    {1, _} ->
        % last message has arrived, we need to conclude things
        {Health, W, Replies} = dict:fold(fun force_reply/3, {ok, W, []},
           DocsDict),
        FinalPSeq = fabric_view_changes:pack_seqs(PSeqsCs),
        {stop, {Health, FinalPSeq, Replies}};
    {_, DocCount} ->
        % we've got at least one reply for each document, let's take a look
        case dict:fold(fun maybe_reply/3, {stop,W,[]}, DocsDict) of
        continue ->
            {ok, {WCount - 1, DocCount, W, NewCounters, {PSeqsCs, DocsDict}}};
        {stop, W, Replies} ->
            FinalPSeq = fabric_view_changes:pack_seqs(PSeqsCs),
            {stop, {ok, FinalPSeq, Replies}}
        end;
    _ ->
        {ok, {WCount - 1, DocCount, W, NewCounters, {PSeqsCs, DocsDict}}}
    end;
handle_message({error, purged_docs_limit_exceeded}=Error, Worker, Acc0) ->
    {WC, LenDocs , W, Counters, {PSeqsCs, DocsDict0}} = Acc0,
    % fill DocsDict with error messages for relevant Docs
    {value, {_W, Docs}, NewCounters} = lists:keytake(Worker, 1, Counters),
    Replies = [Error || _D <- Docs],
    DocsDict = append_update_replies(Docs, Replies, DocsDict0),
    skip_message({WC-1, LenDocs, W, NewCounters, {PSeqsCs, DocsDict}});
handle_message({bad_request, Msg}, _, _) ->
    throw({bad_request, Msg}).


tag_docs(AllIdsRevs) ->
    lists:foldr(fun({Id, Revs}, {TIdsAcc, TIdsRevsAcc, Tag0}) ->
        Tag = Tag0 + 1,
        NewTIdsAcc = [{Tag, Id} | TIdsAcc],
        NewTIdsRevsAcc = [{{Tag,Id}, {Id, Revs}} | TIdsRevsAcc],
        {NewTIdsAcc, NewTIdsRevsAcc, Tag}
    end, {[], [], 0}, AllIdsRevs).


force_reply(Doc, Replies, {Health, W, Acc}) ->
    case update_quorum_met(W, Replies) of
    {true, FinalReply} ->
        {Health, W, [{Doc, FinalReply} | Acc]};
    false ->
        case [Reply || {ok, Reply} <- Replies] of
        [] ->
            UReplies = lists:usort(Replies),
            case UReplies of
                [{error, internal_server_error}] ->
                    {error, W, [{Doc, {error, internal_server_error}} | Acc]};
                [FirstReply|[]] ->
                    % check if all errors are identical, if so inherit health
                    {Health, W, [{Doc, FirstReply} | Acc]};
                _ ->
                    {error, W, [{Doc, UReplies} | Acc]}
             end;

        AcceptedReplies0 ->
            NewHealth = case Health of ok -> accepted; _ -> Health end,
            AcceptedReplies = lists:usort(lists:flatten(AcceptedReplies0)),
            {NewHealth, W, [{Doc, {accepted, AcceptedReplies}} | Acc]}
        end
    end.


maybe_reply(_, _, continue) ->
    % we didn't meet quorum for all docs, so we're fast-forwarding the fold
    continue;
maybe_reply(Doc, Replies, {stop, W, Acc}) ->
    case update_quorum_met(W, Replies) of
    {true, Reply} ->
        {stop, W, [{Doc, Reply} | Acc]};
    false ->
        continue
    end.

update_quorum_met(W, Replies) ->
    OkReplies = lists:foldl(fun(Reply, PrevsAcc) ->
        case Reply of
            {ok, PurgedRevs} -> [PurgedRevs | PrevsAcc];
            _ -> PrevsAcc
        end
    end, [], Replies),
    if length(OkReplies) < W -> false; true ->
        % make a union of PurgedRevs
        FinalReply = {ok, lists:usort(lists:flatten(OkReplies))},
        {true, FinalReply}
    end.


group_idrevs_by_shard(DbName, TIdsRevs) ->
    lists:foldl(fun({{_Tag, Id},_} = TIdRevs, D0) ->
        lists:foldl(fun(Shard, D1) ->
            dict:append(Shard, TIdRevs, D1)
        end, D0, mem3:shards(DbName, Id))
    end, dict:new(), TIdsRevs).


append_update_replies([], [], DocReplyDict) ->
    DocReplyDict;
append_update_replies([Doc|Rest1], [Reply|Rest2], Dict0) ->
    append_update_replies(Rest1, Rest2, dict:append(Doc, Reply, Dict0)).


skip_message({0, _, W, _, {PSeqsCs, DocsDict}}) ->
    {Health, W, Reply} = dict:fold(fun force_reply/3, {ok, W, []}, DocsDict),
    FinalPSeq = fabric_view_changes:pack_seqs(PSeqsCs),
    {stop, {Health, FinalPSeq, Reply}};
skip_message(Acc0) ->
    {ok, Acc0}.


% eunits
doc_purge_ok_test() ->
    meck:new(couch_log),
    meck:expect(couch_log, warning, fun(_,_) -> ok end),
    meck:expect(couch_log, notice, fun(_,_) -> ok end),

    Revs1 = [{1, <<"rev11">>}], IdRevs1 = {<<"id1">>, Revs1},
    Revs2 = [{1, <<"rev12">>}], IdRevs2 = {<<"id2">>, Revs2},
    IdsRevs = [IdRevs1, IdRevs2],
    Shards =
        mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
    GroupedIdsRevs = group_idrevs_by_shard_hack(<<"foo">>, Shards, IdsRevs),
    DocsDict = dict:from_list([{Doc,[]} || Doc <- IdsRevs]),

    % ***test for W = 2
    AccW2 = {length(Shards), length(IdsRevs), list_to_integer("2"),
        dict:to_list(GroupedIdsRevs), {[], DocsDict}},
    {ok, {WaitingCountW2_1,_,_,_,_} = AccW2_1} =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}}, hd(Shards), AccW2),
    ?assertEqual(2, WaitingCountW2_1),
    {stop, FinalReplyW2 } =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}},
            lists:nth(2,Shards), AccW2_1),
    ?assertMatch(
        {ok, _PSeq, Replies} when Replies ==
            [{IdRevs2, {ok, Revs2}}, {IdRevs1, {ok, Revs1}}],
        FinalReplyW2
    ),

    % ***test for W = 3
    AccW3 = {length(Shards), length(IdsRevs), list_to_integer("3"),
        dict:to_list(GroupedIdsRevs), {[], DocsDict}},
    {ok, {WaitingCountW3_1,_,_,_,_} = AccW3_1} =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}}, hd(Shards), AccW3),
    ?assertEqual(2, WaitingCountW3_1),
    {ok, {WaitingCountW3_2,_,_,_,_} = AccW3_2} =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}},
            lists:nth(2,Shards), AccW3_1),
    ?assertEqual(1, WaitingCountW3_2),
    {stop, FinalReplyW3 } =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}},
            lists:nth(3,Shards), AccW3_2),
    ?assertMatch(
        {ok, _PSeq, Replies} when Replies ==
            [{IdRevs2, {ok, Revs2}}, {IdRevs1, {ok, Revs1}}],
        FinalReplyW3
    ),

    % *** test rexi_exit on 1 node
    Acc0 = {length(Shards), length(IdsRevs), list_to_integer("2"),
        dict:to_list(GroupedIdsRevs), {[], DocsDict}},
    {ok, {WaitingCount1,_,_,_,_} = Acc1} =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}}, hd(Shards), Acc0),
    ?assertEqual(2, WaitingCount1),
    {ok, {WaitingCount2,_,_,_,_} = Acc2} =
        handle_message({rexi_EXIT, nil}, lists:nth(2,Shards), Acc1),
    ?assertEqual(1, WaitingCount2),
    {stop, Reply} =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}}, lists:nth(3,Shards), Acc2),
    ?assertMatch(
        {ok, _PSeq, Replies} when Replies ==
            [{IdRevs2, {ok, Revs2}}, {IdRevs1, {ok, Revs1}}],
        Reply
    ),

    % *** test {error, purge_during_compaction_exceeded_limit} on all nodes
    % *** still should return ok reply for the request
    ErrPDCEL = {error, purge_during_compaction_exceeded_limit},
    Acc20 = {length(Shards), length(IdsRevs), list_to_integer("3"),
        dict:to_list(GroupedIdsRevs), {[], DocsDict}},
    {ok, {WaitingCount21,_,_,_,_} = Acc21} =
        handle_message({ok,{0,[ErrPDCEL, ErrPDCEL]}}, hd(Shards), Acc20),
    ?assertEqual(2, WaitingCount21),
    {ok, {WaitingCount22,_,_,_,_} = Acc22} =
        handle_message({ok,{0,[ErrPDCEL, ErrPDCEL]}},
            lists:nth(2,Shards), Acc21),
    ?assertEqual(1, WaitingCount22),
    {stop, Reply2 } =
        handle_message({ok,{0,[ErrPDCEL, ErrPDCEL]}},
            lists:nth(3,Shards), Acc22),
    ?assertMatch(
        {ok, _PSeq, Replies2} when Replies2 ==
            [{IdRevs2, ErrPDCEL}, {IdRevs1, ErrPDCEL}],
        Reply2
    ),

    % *** test {error, purged_docs_limit_exceeded} on all nodes
    % *** still should return ok reply for the request
    ErrPDLE = {error, purged_docs_limit_exceeded},
    Acc30 = {length(Shards), length(IdsRevs), list_to_integer("3"),
        dict:to_list(GroupedIdsRevs), {[], DocsDict}},
    {ok, {WaitingCount31,_,_,_,_} = Acc31} =
        handle_message({ok,{0,[ErrPDLE, ErrPDLE]}}, hd(Shards), Acc30),
    ?assertEqual(2, WaitingCount31),
    {ok, {WaitingCount32,_,_,_,_} = Acc32} =
        handle_message({ok,{0,[ErrPDLE, ErrPDLE]}},
            lists:nth(2,Shards), Acc31),
    ?assertEqual(1, WaitingCount32),
    {stop, Reply3 } =
        handle_message({ok,{0,[ErrPDLE, ErrPDLE]}},
            lists:nth(3,Shards), Acc32),
    ?assertMatch(
        {ok, _PSeq, Replies3} when Replies3 ==
            [{IdRevs2, ErrPDLE}, {IdRevs1, ErrPDLE}],
        Reply3
    ),
    meck:unload(couch_log).


doc_purge_accepted_test() ->
    meck:new(couch_log),
    meck:expect(couch_log, warning, fun(_,_) -> ok end),
    meck:expect(couch_log, notice, fun(_,_) -> ok end),
    Revs1 = [{1, <<"rev11">>}], IdRevs1 = {<<"id1">>, Revs1},
    Revs2 = [{1, <<"rev12">>}], IdRevs2 = {<<"id2">>, Revs2},
    IdsRevs = [IdRevs1, IdRevs2],
    Shards =
        mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
    GroupedIdsRevs = group_idrevs_by_shard_hack(<<"foo">>, Shards, IdsRevs),
    DocsDict = dict:from_list([{Doc,[]} || Doc <- IdsRevs]),

    % *** test rexi_exit on 2 nodes
    Acc0 = {length(Shards), length(IdsRevs), list_to_integer("2"),
        dict:to_list(GroupedIdsRevs), {[], DocsDict}},
    {ok, {WaitingCount1,_,_,_,_} = Acc1} =
        handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}}, hd(Shards), Acc0),
    ?assertEqual(2, WaitingCount1),
    {ok, {WaitingCount2,_,_,_,_} = Acc2} =
        handle_message({rexi_EXIT, nil}, lists:nth(2,Shards), Acc1),
    ?assertEqual(1, WaitingCount2),
    {stop, Reply} =
        handle_message({rexi_EXIT, nil}, lists:nth(3,Shards), Acc2),
    ?assertMatch(
        {accepted, _PSeq, Replies} when Replies ==
            [{IdRevs2, {accepted, Revs2}}, {IdRevs1, {accepted, Revs1}}],
        Reply
    ),
    meck:unload(couch_log).


doc_purge_error_test() ->
    meck:new(couch_log),
    meck:expect(couch_log, warning, fun(_,_) -> ok end),
    meck:expect(couch_log, notice, fun(_,_) -> ok end),
    Revs1 = [{1, <<"rev11">>}], IdRevs1 = {<<"id1">>, Revs1},
    Revs2 = [{1, <<"rev12">>}], IdRevs2 = {<<"id2">>, Revs2},
    IdsRevs = [IdRevs1, IdRevs2],
    Shards =
        mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
    GroupedIdsRevs = group_idrevs_by_shard_hack(<<"foo">>, Shards, IdsRevs),
    DocsDict = dict:from_list([{Doc,[]} || Doc <- IdsRevs]),

    % *** test rexi_exit on 3 nodes
    Acc0 = {length(Shards), length(IdsRevs), list_to_integer("2"),
        dict:to_list(GroupedIdsRevs), {[], DocsDict}},
    {ok, {WaitingCount1,_,_,_,_} = Acc1} =
        handle_message({rexi_EXIT, nil}, hd(Shards), Acc0),
    ?assertEqual(2, WaitingCount1),
    {ok, {WaitingCount2,_,_,_,_} = Acc2} =
        handle_message({rexi_EXIT, nil}, lists:nth(2,Shards), Acc1),
    ?assertEqual(1, WaitingCount2),
    {stop, Reply} =
        handle_message({rexi_EXIT, nil}, lists:nth(3,Shards), Acc2),
    ?assertMatch(
        {error, _PSeq, Replies} when Replies ==
            [{IdRevs2, {error, internal_server_error}},
            {IdRevs1, {error, internal_server_error}}],
        Reply
    ),

    % ***test w quorum > # shards, which should fail immediately
    Shards2 = mem3_util:create_partition_map("foo",1,1,["node1"]),
    GroupedIdsRevs2 = group_idrevs_by_shard_hack(<<"foo">>, Shards2, IdsRevs),
    AccW4 = {length(Shards2), length(IdsRevs), list_to_integer("2"),
        dict:to_list(GroupedIdsRevs2), {[], DocsDict}},
    Bool =
        case handle_message({ok,{2,[{ok, Revs1}, {ok, Revs2}]}}, hd(Shards), AccW4) of
            {stop, _Reply} ->
                true;
            _ -> false
        end,
    ?assertEqual(true, Bool),

    % *** test Docs with no replies should end up as {error, internal_server_error}
    SA1 = #shard{node=a, range=1},
    SA2 = #shard{node=a, range=2},
    SB1 = #shard{node=b, range=1},
    SB2 = #shard{node=b, range=2},
    GroupedIdsRevs3 = [{SA1,[IdRevs1]}, {SB1,[IdRevs1]},
        {SA2,[IdRevs2]}, {SB2,[IdRevs2]}],
    DocsDict = dict:from_list([{Doc,[]} || Doc <- IdsRevs]),
    Acc30 = {length(GroupedIdsRevs3), length(IdsRevs), 2,
        GroupedIdsRevs3, {[], DocsDict}},
    {ok, Acc31} = handle_message({ok,{1,[{ok, Revs1}]}}, SA1, Acc30),
    {ok, Acc32} = handle_message({rexi_EXIT, nil}, SB1, Acc31),
    {ok, Acc33} = handle_message({rexi_EXIT, nil}, SA2, Acc32),
    {stop, Acc34} = handle_message({rexi_EXIT, nil}, SB2, Acc33),
    ?assertMatch(
        {error, _PSeq, Replies3} when Replies3 ==
            [{IdRevs2, {error, internal_server_error}},
            {IdRevs1, {accepted, Revs1}}],
        Acc34
    ),
    meck:unload(couch_log).


% needed for testing to avoid having to start the mem3 application
group_idrevs_by_shard_hack(_DbName, Shards, TIdsRevs) ->
    lists:foldl(fun(IdRevs, D0) ->
        lists:foldl(fun(Shard, D1) ->
            dict:append(Shard, IdRevs, D1)
        end, D0, Shards)
    end, dict:new(), TIdsRevs).
