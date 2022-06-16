:- module(rocks_preds,
          [ rdb_open/2,                 % +Dir, -DB
            rdb_assertz/1,              % +Clause
            rdb_assertz/2,              % +Dir, +Clause
            rdb_clause/2,               % +Head,-Body
            rdb_clause/3,               % +Dir, +Head, -Body
            rdb_clause/4,               % +Dir, +Head, -Body, ?CRef
            rdb_load_file/1,            % +File
            rdb_load_file/2,            % +Dir, +File
            rdb_current_predicate/1,    % ?PI
            rdb_current_predicate/2,    % +Dir,?PI
            rdb_index/2,                % :PI, +Spec
            rdb_index/3,                % +Dir, :PI, +Spec
            rdb_destroy_index/2,        % :PI,+Spec
            rdb_destroy_index/3         % +Dir,:PI,+Spec
          ]).
:- use_module(library(rocksdb)).
:- use_module(library(prolog_code)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(lists)).

/** <module> Store full predicates in a RocksDB

*/

:- meta_predicate
    rdb_assertz(:),
    rdb_assertz(+, :),
    rdb_current_predicate(:),
    rdb_current_predicate(+, :),
    rdb_clause(:, -),
    rdb_clause(+, :, -),
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
    rocks_put(DB, KeyClause, (Head:-Body)).

register_predicate(DB, PI) :-
    pred_current_key(PI, Key),
    rocks_put(DB, Key, true).

%!  rdb_current_predicate(?PI) is nondet.
%!  rdb_current_predicate(+Dir, ?PI) is nondet.

rdb_current_predicate(PI) :-
    default_db(Dir),
    rdb_current_predicate(Dir, PI).

rdb_current_predicate(Dir, PI), ground(PI) =>
    rdb_open(Dir, DB),
    pred_current_key(PI, Key),
    rocks_get(DB, Key, true).
rdb_current_predicate(Dir, PI) =>
    rdb_open(Dir, DB),
    pred_current_prefix(Prefix),
    rocks_enum_from(DB, Key, true, Prefix),
    (   sub_string(Key, 0, _, After, Prefix)
    ->  sub_string(Key, _, After, 0, PIs),
        term_string(PI, PIs)
    ;   !, fail
    ).


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
        rdb_candidates(DB, Index, Head, Candidates)
    ->  member(CRef, Candidates),
        rocks_get(DB, CRef, (Head :- Body))
    ;   pred_clause_prefix(PI, Prefix),
        rocks_enum_from(DB, CRef, (Head:-Body), Prefix),
        (   sub_string(CRef, 0, _, _, Prefix)
        ->  true
        ;   !, fail
        )
    ).

clause_head_body((Head0 :- Body0), Head, Body) =>
    Head = Head0,
    Body = Body0.
clause_head_body(Head0, Head, Body) =>
    Head = Head0,
    Body = true.

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
    rocks_put(DB, KeyIndex, true).

add_to_clause_index(DB, PI, Spec, _:Head, CRef), integer(Spec) =>
    arg(Spec, Head, Arg),
    term_hash(Arg, 1, 2147483647, Hash),
    assertion(nonvar(Hash)),            % TBD: variables in the head
    pred_index_key(PI, Spec, Hash, Key),
    (   rocks_get(DB, Key, Clauses)
    ->  true
    ;   Clauses = []
    ),
    rocks_put(DB, Key, [CRef|Clauses]).

%!  rdb_destroy_index(:PI, +Spec) is det.
%!  rdb_destroy_index(+Dir, :PI, +Spec) is det.

rdb_destroy_index(PI, Spec) :-
    default_db(Dir),
    rdb_destroy_index(Dir, PI, Spec).

rdb_destroy_index(Dir, PI, Spec) :-
    rdb_open(Dir, DB),
    pred_index_key(PI, Spec, KeyIndex),
    rocks_put(DB, KeyIndex, destroying),
    pred_index_prefix(PI, Spec, Prefix),
    (   rocks_enum_from(DB, Key, _, Prefix),
        (   sub_string(Key, 0, _, _, Prefix)
        ->  rocks_delete(DB, Key),
            fail
        ;   !
        )
    ;   true
    ),
    rocks_delete(DB, KeyIndex).


%!  rdb_clause_index(+DB, :PI, -Index) is nondet.
%
%   True when Index is a  clause   index  specification on the predicate
%   PI.

rdb_clause_index(DB, PI, Index) :-
    pred_index_prefix(PI, Prefix),
    rocks_enum_from(DB, Key, true, Prefix),
    (   sub_string(Key, 0, _, After, Prefix)
    ->  sub_string(Key, _, After, 0, IndexS),
        term_string(Index, IndexS)
    ;   !, fail
    ).

%!  rdb_candidates(+DB, +Spec, :Head, -Candidates) is semidet.

rdb_candidates(DB, Spec, M:Head, Candidates), integer(Spec) =>
    arg(Spec, Head, Arg),
    nonvar(Arg),
    term_hash(Arg, 1, 2147483647, Hash),
    nonvar(Hash),
    pi_head(PI, M:Head),
    pred_index_key(PI, Spec, Hash, Key),
    rocks_get(DB, Key, Candidates).


		 /*******************************
		 *        TRIPLE DATABASE	*
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

pred_index_prefix(PI, Spec, Key) :-
    format(string(Key), '~q\u0004~q\u0002', [PI, Spec]).

pred_index_key(PI, Spec, Hash, Key) :-
    format(string(Key), '~q\u0004~q\u0002~16r', [PI,Spec,Hash]).

%!  rdb_open(+Directory, -DB)
%
%   Open a database for persistent predicates

rdb_open(Dir, DB) :-
    pred_table(Dir, DB),
    !.
rdb_open(Dir, DB) :-
    ensure_directory(Dir),
    directory_file_path(Dir, predicates, PredName),
    rocks_open(PredName,  DB,
               [ key(string), value(term),
                 alias(predicates)
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
