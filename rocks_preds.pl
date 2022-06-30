/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        jan@swi-prolog.org
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2022, SWI-Prolog Solutions b.v.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

:- module(rocks_preds,
          [ rdb_open/2,                 % +Dir, -DB
            rdb_assertz/1,              % :Clause
            rdb_assertz/2,              % +Dir, :Clause
            rdb_retract/1,              % :Clause
            rdb_retract/2,              % +Dir, :Clause
            rdb_retractall/1,           % :Head
            rdb_retractall/2,           % +Dir, :Head
            rdb_clause/2,               % :Head, -Body
            rdb_clause/3,               % +Dir, :Head, -Body
            rdb_clause/4,               % +Dir, :Head, -Body, ?CRef
            rdb_nth_clause/3,           % :Head, ?Nth, ?Reference
            rdb_nth_clause/4,           % +Dir, :Head, ?Nth, ?Reference
            rdb_load_file/1,            % +File
            rdb_load_file/2,            % +Dir, +File
            rdb_current_predicate/1,    % ?PI
            rdb_current_predicate/2,    % +Dir,?PI
            rdb_predicate_property/2,   % :Head, ?Property
            rdb_predicate_property/3,   % ?Dir, :Head, ?Property
            rdb_index/2,                % :PI, +Spec
            rdb_index/3,                % +Dir, :PI, +Spec
            rdb_destroy_index/2,        % :PI,+Spec
            rdb_destroy_index/3         % +Dir,:PI,+Spec
          ]).
:- use_module(library(rocksdb)).
:- use_module(library(prolog_code)).
:- use_module(library(debug)).
:- use_module(library(filesex)).

/** <module> Store full predicates in a RocksDB

*/

:- meta_predicate
    rdb_assertz(:),
    rdb_assertz(+, :),
    rdb_retract(:),
    rdb_retract(+, :),
    rdb_retractall(:),
    rdb_retractall(+, :),
    rdb_current_predicate(:),
    rdb_current_predicate(+, :),
    rdb_predicate_property(:, ?),
    rdb_predicate_property(?, :, ?),
    rdb_clause(:, -),
    rdb_clause(+, :, -),
    rdb_nth_clause(:, ?, ?),
    rdb_nth_clause(+, :, ?, ?),
    rdb_clause(+, :, -, ?),
    rdb_load_file(:),
    rdb_index(:, +),
    rdb_index(+, :, +),
    rdb_destroy_index(:, +),
    rdb_destroy_index(+, :, +).

:- dynamic
    pred_table/2.                     % Dir, Table

default_db(DB) :-
    pred_table(DB, _),
    !.
default_db('predicates.db').

%!  rdb_assertz(+Dir, +Clause) is det.
%
%   Store a clause in the persistent database

rdb_assertz(Clause) :-
    default_db(Dir),
    rdb_assertz(Dir, Clause).

rdb_assertz(Dir, Clause) :-
    rdb_open(Dir, DB),
    clause_head_body(Clause, Head, Body),
    pi_head(PI, Head),
    pred_property_key(PI, last_clause, KeyLC),
    (   rocks_get(DB, KeyLC, Id)
    ->  NId is Id+1
    ;   register_predicate(DB, PI),
        NId is 1
    ),
    rocks_put(DB, KeyLC, NId),
    pred_clause_key(PI, NId, KeyClause),
    rocks_put(DB, KeyClause, (Head:-Body)),
    add_to_indexes(DB, PI, Head, KeyClause).

register_predicate(DB, PI) :-
    pred_current_key(PI, Key),
    rocks_put(DB, Key, true).

%!  rdb_retract(:Clause) is nondet.
%!  rdb_retract(+Dir, :Clause) is nondet.

rdb_retract(Clause) :-
    default_db(Dir),
    rdb_retract(Dir, Clause).

rdb_retract(Dir, Clause) :-
    rdb_open(Dir, DB),
    clause_head_body(Clause, Head, Body),
    pi_head(PI, Head),
    pred_property_key(PI, erased, KeyErased),
    rdb_clause(Dir, Head, Body, CRef),
    rocks_delete(DB, CRef),
    delete_from_indexes(DB, PI, Head, CRef),
    (   rocks_get(DB, KeyErased, Erased0)
    ->  Erased is Erased0+1
    ;   Erased is 1
    ),
    rocks_put(DB, KeyErased, Erased).

%!  rdb_retractall(:Head) is det.
%!  rdb_retractall(+Dir, :Head) is det.

rdb_retractall(Head) :-
    default_db(Dir),
    rdb_retractall(Dir, Head).

rdb_retractall(Dir, Head) :-
    forall(rdb_retract(Dir, (Head :- _)), true).

%!  rdb_current_predicate(?PI) is nondet.
%!  rdb_current_predicate(?Dir, ?PI) is nondet.

rdb_current_predicate(PI) :-
    default_db(Dir),
    rdb_current_predicate(Dir, PI).

rdb_current_predicate(Dir, PI), ground(PI) =>
    pred_table(Dir, DB),
    pred_current_key(PI, Key),
    rocks_get(DB, Key, true).
rdb_current_predicate(Dir, PI) =>
    pred_table(Dir, DB),
    pred_current_prefix(Prefix),
    rocks_enum_prefix(DB, PIs, true, Prefix),
    term_string(PI, PIs).


%!  rdb_clause(:Head, -Body) is nondet.
%!  rdb_clause(+Dir, :Head, -Body) is nondet.
%!  rdb_clause(+Dir, :Head, -Body, ?CRef) is nondet.
%
%   Retrieve a clause from the persistent database

rdb_clause(Head, Body) :-
    default_db(Dir),
    rdb_clause(Dir, Head, Body).
rdb_clause(Dir, Head, Body) :-
    rdb_clause(Dir, Head, Body, _).

rdb_clause(Dir, Head, Body, CRef), nonvar(CRef) =>
    rdb_open(Dir, DB),
    rocks_get(DB, CRef, (Head :- Body)).
rdb_clause(Dir, Head, Body, CRef) =>
    rdb_open(Dir, DB),
    pi_head(PI, Head),
    (   rdb_clause_index(DB, PI, Index),
        rdb_candidates(DB, Index, Head, IndexPrefix)
    ->  rdb_candidate(DB, IndexPrefix, CRef),
        rocks_get(DB, CRef, (Head :- Body))
    ;   pred_clause_prefix(PI, Prefix),
        rocks_enum_prefix(DB, Suffix, (Head:-Body), Prefix),
        string_concat(Prefix, Suffix, CRef)
    ).

clause_head_body((Head0 :- Body0), Head, Body) =>
    Head = Head0,
    Body = Body0.
clause_head_body(Head0, Head, Body) =>
    Head = Head0,
    Body = true.

%!  rdb_nth_clause(+Head, ?Nth, ?Reference) is nondet.
%!  rdb_nth_clause(+Dir, +Head, ?Nth, ?Reference) is nondet.
%
%   True when Reference is the clause reference   for  the Nth clause in
%   Head.

rdb_nth_clause(Head, Nth, Reference) :-
    default_db(Dir),
    rdb_nth_clause(Dir, Head, Nth, Reference).

rdb_nth_clause(Dir, Head, Nth, Cref) =>
    pi_head(PI, Head),
    pred_property_key(PI, last_clause, KeyLC),
    rdb_open(Dir, DB),
    rocks_get(DB, KeyLC, Max),
    between(1, Max, Nth),
    pred_clause_key(PI, Nth, Cref),
    rocks_get(DB, Cref, (Head :- _)).


%!  rdb_load_file(:File) is det.
%!  rdb_load_file(+Dir, :File) is det.
%
%   Load all clauses from File  into   a  persistent database. Note this
%   does not (yet) deal with directives, term expansion, etc.

rdb_load_file(File) :-
    default_db(Dir),
    rdb_load_file(Dir, File).

rdb_load_file(Dir, M:File) :-
    get_time(T0),
    absolute_file_name(File, FullFile,
                       [ file_type(prolog),
                         access(read)
                       ]),
    setup_call_cleanup(
        open(FullFile, read, In),
        rdb_load_stream(Dir, In, M, Clauses),
        close(In)),
    get_time(T1),
    T is T1-T0,
    print_message(informational, rdb_load_file(FullFile, Clauses, T)).

rdb_load_stream(Dir, In, M, Count) :-
    read_term(In, T0, []),
    load_stream(T0, Dir, In, M, 0, Count).

load_stream(end_of_file, _, _, _, Count, Count) :-
    !.
load_stream(T, Dir, In, M, N0, N) :-
    rdb_assertz(Dir, M:T),
    read_term(In, T2, []),
    N1 is N0+1,
    load_stream(T2, Dir, In, M, N1, N).


		 /*******************************
		 *           INDEXING		*
		 *******************************/

%!  rdb_index(:PI, +Spec) is det.
%!  rdb_index(+Dir, :PI, +Spec) is det.
%
%   Add an index for the predicate Head.  Spec is one of:
%
%     - An integer
%       Create an index for the Nth argument.

rdb_index(PI, Spec) :-
    default_db(Dir),
    rdb_index(Dir, PI, Spec).

rdb_index(Dir, PI, Spec), integer(Spec) =>
    pi_head(PI, Head),
    rdb_open(Dir, DB),
    pred_index_key(PI, Spec, KeyIndex),
    rocks_put(DB, KeyIndex, indexing),
    forall(rdb_clause(Dir, Head, _, CRef),
           add_to_clause_index(DB, PI, Spec, Head, CRef)),
    rocks_put(DB, KeyIndex, true),
    flush_index_cache(DB, PI).

add_to_clause_index(DB, PI, Spec, _:Head, CRef), integer(Spec) =>
    arg(Spec, Head, Arg),
    term_hash(Arg, 1, 2147483647, Hash),
    assertion(nonvar(Hash)),            % TBD: variables in the head
    pred_index_key(PI, Spec, Hash, CRef, Key),
    rocks_put(DB, Key, 1).

delete_from_clause_index(DB, PI, Spec, _:Head, CRef), integer(Spec) =>
    arg(Spec, Head, Arg),
    term_hash(Arg, 1, 2147483647, Hash),
    assertion(nonvar(Hash)),            % TBD: variables in the head
    pred_index_key(PI, Spec, Hash, CRef, Key),
    rocks_delete(DB, Key).

add_to_indexes(DB, PI, Head, CRef) :-
    forall(rdb_clause_index(DB, PI, Index),
           add_to_clause_index(DB, PI, Index, _:Head, CRef)).

delete_from_indexes(DB, PI, Head, CRef) :-
    forall(rdb_clause_index(DB, PI, Index),
           delete_from_clause_index(DB, PI, Index, _:Head, CRef)).

%!  rdb_destroy_index(:PI, +Spec) is det.
%!  rdb_destroy_index(+Dir, :PI, +Spec) is det.

rdb_destroy_index(PI, Spec) :-
    default_db(Dir),
    rdb_destroy_index(Dir, PI, Spec).

rdb_destroy_index(Dir, PI, Spec) :-
    rdb_open(Dir, DB),
    pred_index_key(PI, Spec, KeyIndex),
    rocks_put(DB, KeyIndex, destroying),
    pred_indexes_prefix(PI, Spec, Prefix),
    (   rocks_enum_from(DB, Key, _, Prefix),
        (   sub_string(Key, 0, _, _, Prefix)
        ->  rocks_delete(DB, Key),
            fail
        ;   !
        )
    ;   true
    ),
    rocks_delete(DB, KeyIndex),
    flush_index_cache(DB, PI).


%!  rdb_clause_index(+DB, :PI, -Index) is nondet.
%
%   True when Index is a  clause   index  specification on the predicate
%   PI.

:- table
    rdb_clause_index/3.

rdb_clause_index(DB, PI, Index) :-
    pred_index_prefix(PI, Prefix),
    rocks_enum_prefix(DB, IndexS, true, Prefix),
    term_string(Index, IndexS).

flush_index_cache(DB, PI) :-
    abolish_table_subgoals(rdb_clause_index(DB, PI, _)).

%!  rdb_candidates(+DB, +Spec, :Head, -Prefix) is semidet.

rdb_candidates(_DB, Spec, M:Head, Prefix), integer(Spec) =>
    arg(Spec, Head, Arg),
    nonvar(Arg),
    term_hash(Arg, 1, 2147483647, Hash),
    nonvar(Hash),
    pi_head(PI, M:Head),
    pred_index_prefix(PI, Spec, Hash, Prefix).

rdb_candidate(DB, Prefix, Candidate) :-
    rocks_enum_prefix(DB, Candidate, 1, Prefix).


%!  rdb_predicate_property(:Head, ?Property) is nondet.
%!  rdb_predicate_property(?Dir, :Head, ?Property) is nondet.
%
%   Query properties of a persistent predicate

rdb_predicate_property(Head, Property) :-
    default_db(Dir),
    rdb_predicate_property(Dir, Head, Property).

rdb_predicate_property(Dir, M:Head, Property), var(Head) =>
    rdb_current_predicate(Dir, PI),
    pi_head(PI, M:Head),
    property(Property, Dir, PI).
rdb_predicate_property(Dir, Head, Property) =>
    pi_head(PI, Head),
    rdb_current_predicate(Dir, PI),
    property(Property, Dir, PI).

property(database(DB), DB, _).
property(defined, _, _).
property(indexed(Indexes), Dir, PI) :-
    rdb_open(Dir, DB),
    findall(Index, rdb_clause_index(DB, PI, Index), Indexes),
    Indexes \== [].
property(number_of_clauses(N), Dir, PI) :-
    rdb_open(Dir, DB),
    pred_property_key(PI, last_clause, KeyLC),
    pred_property_key(PI, erased, KeyErased),
    rocks_get(DB, KeyLC, N0),
    (   rocks_get(DB, KeyErased, Erased)
    ->  N is N0-Erased
    ;   N = N0
    ).


		 /*******************************
		 *             KEYS		*
		 *******************************/

pred_clause_key(PI, Nth, Key) :-
    format(string(Key), '~q\u0001~|~`0t~16r~10+', [PI,Nth]).

pred_clause_prefix(PI, Prefix) :-
    format(string(Prefix), '~q\u0001', [PI]).

pred_current_key(PI, Key) :-
    format(string(Key), 'meta\u0001~q', [PI]).

pred_current_prefix("meta\u0001").

pred_property_key(PI, Prop, Key) :-
    format(string(Key), '~q\u0002~w', [PI,Prop]).

pred_index_prefix(PI, Key) :-
    format(string(Key), '~q\u0003', [PI]).

pred_index_key(PI, Spec, Key) :-
    format(string(Key), '~q\u0003~q', [PI,Spec]).

pred_indexes_prefix(PI, Spec, Key) :-
    format(string(Key), '~q\u0004~q\u0002', [PI, Spec]).

pred_index_prefix(PI, Spec, Hash, Key) :-
    format(string(Key), '~q\u0004~q\u0002~16r\u0001', [PI,Spec,Hash]).

pred_index_key(PI, Spec, Hash, CRef, Key) :-
    format(string(Key), '~q\u0004~q\u0002~16r\u0001~w', [PI,Spec,Hash,CRef]).

%!  rdb_open(+Directory, -DB)
%
%   Open a database for persistent  predicates.   We  use one additional
%   level of directories such that  we   can  add  multiple databases or
%   additional information to the primary RocksDB database.

rdb_open(Dir, DB) :-
    pred_table(Dir, DB),
    !.
rdb_open(Dir, DB) :-
    ensure_directory(Dir),
    directory_file_path(Dir, predicates, PredName),
    rocks_open(PredName,  DB,
               [ key(string), value(term)
               ]),
    asserta(pred_table(Dir, DB)).

ensure_directory(Dir) :-
    exists_directory(Dir),
    !.
ensure_directory(Dir) :-
    make_directory(Dir).


		 /*******************************
		 *            DEBUGGING		*
		 *******************************/

		 /*******************************
		 *           MESSAGES		*
		 *******************************/

:- multifile prolog:message//1.

prolog:message(rdb_load_file(File, Clauses, T)) -->
    [ 'Loaded ~D clauses from ~w in ~3f seconds'-[Clauses, File, T] ].
