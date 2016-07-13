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

-module(fabric_local).


-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include("fabric.hrl").


% Supported Fabric APIs
-export([
    get_db_info/1,
    get_doc_count/1,
    get_revs_limit/1,
    get_security/1,
    get_security/2,

    open_doc/3,
    open_revs/4,
    get_doc_info/3,
    get_full_doc_info/3,
    get_missing_revs/2,
    get_missing_revs/3,

    all_docs/4,
    all_docs/5,

    changes/4,

    query_view/3,
    query_view/4,
    query_view/6,

    get_view_group_info/2,

    design_docs/1
]).


% Unsupported
-export([
    all_dbs/0,
    all_dbs/1,
    create_db/1,
    create_db/2,
    delete_db/1,
    delete_db/2,
    set_revs_limit/3,
    set_security/2,
    set_security/3,
    get_all_security/1,
    get_all_security/2,
    update_doc/3,
    update_docs/3,
    purge_docs/2,
    att_receiver/2,
    end_changes/0,
    reset_validation_funs/1,
    cleanup_index_files/0,
    cleanup_index_files/1,
    dbname/1
]).


get_db_info(Db) ->
    couch_db:get_db_info(Db).


get_doc_count(Db) ->
    couch_db:get_doc_count(Db).


get_revs_limit(Db) ->
    couch_db:get_revs_limit(Db).


get_security(Db) ->
    get_security(Db, []).


get_security(Db, _Opts) ->
    couch_db:get_security(Db).


open_doc(Db, DocId, Opts) ->
    couch_db:open_doc(Db, DocId, Opts).


open_revs(Db, DocId, Revs, Opts) ->
    couch_db:open_doc_revs(Db, DocId, Revs, Opts).


get_doc_info(Db, DocId, _Opts) ->
    couch_db:get_doc_info(Db, DocId).


get_full_doc_info(Db, DocId, _Opts) ->
    couch_db:get_full_doc_info(Db, DocId).


get_missing_revs(Db, IdsRevs) ->
    get_missing_revs(Db, IdsRevs, []).


get_missing_revs(Db, IdRevsList, _Opts) ->
    Ids = [Id1 || {Id1, _Revs} <- IdRevsList],
    {ok, lists:zipwith(fun({Id, Revs}, FullDocInfoResult) ->
        case FullDocInfoResult of
        {ok, #full_doc_info{rev_tree=RevisionTree} = FullInfo} ->
            MissingRevs = couch_key_tree:find_missing(RevisionTree, Revs),
            {Id, MissingRevs, possible_ancestors(FullInfo, MissingRevs)};
        not_found ->
            {Id, Revs, []}
        end
    end, IdRevsList, couch_btree:lookup(Db#db.id_tree, Ids))}.


all_docs(Db, UserFun, UserAcc, QueryArgs) ->
    all_docs(Db, [], UserFun, UserAcc, QueryArgs).


all_docs(Db, _Opts, UserFun, UserAcc, Args) ->
    couch_mrview:query_all_docs(Db, Args, UserFun, UserAcc).


changes(Db, UserFun, UserAcc, Args) ->
    Pending = couch_db:count_changes_since(Db, Args#changes_args.since),
    ChangesFun = couch_changes:handle_changes(Args, Db, db),
    AccIn = {UserFun, UserAcc, Pending},
    {_, FinalUserAcc, _} = ChangesFun({fun changes_enum/3, AccIn}),
    {ok, FinalUserAcc}.


query_view(Db, DesignName, ViewName) ->
    query_view(Db, DesignName, ViewName, #mrargs{}).


query_view(Db, DesignName, ViewName, Args) ->
    query_view(Db, DesignName, ViewName, fun default_callback/2, [], Args).


query_view(Db, DesignName, ViewName, UserFun, UserAcc, Args)
        when is_binary(DesignName) ->
    DbName = mem3:dbname(Db#db.name),
    {ok, DDoc} = ddoc_cache:open(DbName, <<"_design/", DesignName/binary>>),
    query_view(DbName, DDoc, ViewName, UserFun, UserAcc, Args);

query_view(Db, DDoc, ViewName, UserFun, UserAcc, Args) ->
    couch_mrview:query_view(Db, DDoc, ViewName, Args, UserFun, UserAcc).


get_view_group_info(Db, DDocOrId) ->
    DDocId = case DDocOrId of
        #doc{id = Id} ->
            Id;
        <<"_design/", _/binary>> ->
            DDocOrId;
        Id when is_binary(Id) ->
            <<"_design/", Id/binary>>;
        _ ->
            throw({bad_request, invalid_group_id})
    end,
    couch_mrview:get_info(Db, DDocId).


design_docs(Db) ->
    couch_db:get_design_docs(Db).


default_callback(complete, Acc) ->
    {ok, lists:reverse(Acc)};
default_callback(Row, Acc) ->
    {ok, [Row | Acc]}.



changes_enum({change, Change, _}, RT, Acc) ->
    changes_enum({change, Change}, RT, Acc);

changes_enum({stop, EndSeq}, RT, {_, _, Pending} = Acc) ->
    changes_enum({stop, EndSeq, Pending}, RT, Acc);

changes_enum(Event, _RespType, {UserFun, UserAcc, Pending}) ->
    NewPending = case Event of
        {change, _} -> Pending - 1;
        {change, _, _} -> Pending - 1;
        _ -> Pending
    end,
    {ok, NewUserAcc} = UserFun(Event, UserAcc),
    {UserFun, NewUserAcc, NewPending}.


possible_ancestors(_FullInfo, []) ->
    [];
possible_ancestors(FullInfo, MissingRevs) ->
    #doc_info{revs=RevsInfo} = couch_doc:to_doc_info(FullInfo),
    LeafRevs = [Rev || #rev_info{rev=Rev} <- RevsInfo],
    % Find the revs that are possible parents of this rev
    lists:foldl(fun({LeafPos, LeafRevId}, Acc) ->
        % this leaf is a "possible ancenstor" of the missing
        % revs if this LeafPos lessthan any of the missing revs
        case lists:any(fun({MissingPos, _}) ->
                LeafPos < MissingPos end, MissingRevs) of
        true ->
            [{LeafPos, LeafRevId} | Acc];
        false ->
            Acc
        end
    end, [], LeafRevs).


-define(NI, throw(not_implemented)).


all_dbs() -> ?NI.
all_dbs(_) -> ?NI.
create_db(_) -> ?NI.
create_db(_, _) -> ?NI.
delete_db(_) -> ?NI.
delete_db(_, _) -> ?NI.
set_revs_limit(_, _, _) -> ?NI.
set_security(_, _) -> ?NI.
set_security(_, _, _) -> ?NI.
get_all_security(_) -> ?NI.
get_all_security(_, _) -> ?NI.
update_doc(_, _, _) -> ?NI.
update_docs(_, _, _) -> ?NI.
purge_docs(_, _) -> ?NI.
att_receiver(_, _) -> ?NI.
end_changes() -> ?NI.
reset_validation_funs(_) -> ?NI.
cleanup_index_files() -> ?NI.
cleanup_index_files(_) -> ?NI.
dbname(_) -> ?NI.
