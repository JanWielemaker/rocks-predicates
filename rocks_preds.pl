:- module(rocks_preds,
          [ rdb_open/1,                 % +Directory
            rdb_assertz/1,              % +Clause
            rdb_clause/2,               % +Head,-Body
            rdb_load_file/1             % +File
          ]).
:- use_module(library(rocksdb)).
:- use_module(library(prolog_code)).

/** <module> Store full predicates in a RocksDB

Databases:

  - Term --> id
  - S,P  --> O


*/

:- meta_predicate
    rdb_assertz(:),
    rdb_clause(:, -),
    rdb_load_file(:).


:- dynamic
    intern_table/1,
    extern_table/1,
    triple_table/1.

%!  rdb_assertz(+Clause) is det.
%
%   Store a clause in the persistent database

rdb_assertz(Clause) :-
    clause_head_body(Clause, Head, Body),
    pi_head(PI, Head),
    intern(PI, PID),
    intern(last_clause_id, LID),
    (   t_intern(PID, LID, Id)
    ->  NId is Id+1
    ;   NId is 1
    ),
    put_intern(PID, LID, NId),
    intern((Head :- Body), CID),
    put_intern(PID, NId, CID).

%!  rdb_clause(+Head, -Body) is nondet.
%
%   Retrieve a clause from the persistent database

rdb_clause(Head, Body) :-
    pi_head(PI, Head),
    intern(PI, PID),
    intern(last_clause_id, LID),
    t_intern(PID, LID, LastId),
    between(1, LastId, ClauseNo),
    t_intern(PID, ClauseNo, CID),
    extern(CID, (Head :- Body)).

clause_head_body((Head0 :- Body0), Head, Body) =>
    Head = Head0,
    Body = Body0.
clause_head_body(Head0, Head, Body) =>
    Head = Head0,
    Body = true.

%!  rdb_load_file(:File) is det.
%
%   Load all clauses from File  into   a  persistent database. Note this
%   does not (yet) deal with directives, term expansion, etc.

rdb_load_file(M:File) :-
    get_time(T0),
    absolute_file_name(File, FullFile,
                       [ file_type(prolog),
                         access(read)
                       ]),
    setup_call_cleanup(
        open(FullFile, read, In),
        rdb_load_stream(In, M, Clauses),
        close(In)),
    get_time(T1),
    T is T1-T0,
    print_message(informational, rdb_load_file(FullFile, Clauses, T)).

rdb_load_stream(In, M, Count) :-
    read_term(In, T0, []),
    load_stream(T0, In, M, 0, Count).

load_stream(end_of_file, _, _, Count, Count) :-
    !.
load_stream(T, In, M, N0, N) :-
    rdb_assertz(M:T),
    read_term(In, T2, []),
    N1 is N0+1,
    load_stream(T2, In, M, N1, N).


		 /*******************************
		 *        TRIPLE DATABASE	*
		 *******************************/

t(S,P,O) :-
    intern(S, Sid),
    intern(P, Pid),
    t_intern(Sid, Pid, OId),
    extern(OId, O).

t_intern(S,P,O) :-
    triple_table(DB),
    s_p_sp(S, P, SP),
    rocks_get(DB, SP, O).

put(S,P,O) :-
    intern(S, Sid),
    intern(P, Pid),
    intern(O, Oid),
    put_intern(Sid, Pid, Oid).

put_intern(Sid, Pid, Oid) :-
    s_p_sp(Sid, Pid, SP),
    triple_table(DB),
    rocks_put(DB, SP, Oid).

intern(Term, Id) :-
    intern_table(DB),
    (   rocks_get(DB, Term, Id0)
    ->  Id = Id0
    ;   extern_table(EDB),
        (   rocks_get(DB, '$$LastID', Id0)
        ->  Id is Id0+1
        ;   Id is 1
        ),
        rocks_put(DB, '$$LastID', Id),
        rocks_put(DB, Term, Id),
        rocks_put(EDB, Id, Term)
    ).

extern(Id, Term) :-
    extern_table(EDB),
    rocks_get(EDB, Id, Term).

s_p_sp(S,P,SP) :-
    SP is S<<40+P.
sp_s_p(SP,S,P) :-
    S is SP>>40,
    P is SP/\0xffffff.

%!  rdb_open(+Directory)
%
%   Open a database for persistent predicates

rdb_open(_Dir) :-
    intern_table(_),
    !.
rdb_open(Dir) :-
    ensure_directory(Dir),
    directory_file_path(Dir, intern, InternName),
    directory_file_path(Dir, extern, ExternName),
    directory_file_path(Dir, triple, TripleName),
    rocks_open(InternName, IDB, [ key(term), value(int64) ]),
    rocks_open(ExternName, EDB, [ key(int64), value(term) ]),
    rocks_open(TripleName, TDB, [ key(int64), value(int64) ]),
    asserta(intern_table(IDB)),
    asserta(extern_table(EDB)),
    asserta(triple_table(TDB)).

ensure_directory(Dir) :-
    exists_directory(Dir),
    !.
ensure_directory(Dir) :-
    make_directory(Dir).


		 /*******************************
		 *            DEBUGGING		*
		 *******************************/

rdb_list_intern :-
    intern_table(IDB),
    extern_table(EDB),
    forall((rocks_enum(IDB, Term, Id), Term \== '$$LastID'),
           ( assertion((rocks_get(EDB, Id, Term2), Term2 =@= Term)),
             format('~p ~t~20|<-> ~p~n', [Id, Term]))).

rdb_list_triples :-
    triple_table(TDB),
    forall(rocks_enum(TDB, SP, O),
           list_triple(SP, O)).

list_triple(SP, Oid) :-
    sp_s_p(SP,Sid,Pid),
    extern_or_plain(Sid, S),
    extern_or_plain(Pid, P),
    extern_or_plain(Oid, O),
    format('~p ~t~20|~p ~t~40|~p~n', [S,P,O]).

extern_or_plain(Id, Term) :-
    (   extern(Id, Term0)
    ->  Term = Term0
    ;   Term = id(Id)
    ).


		 /*******************************
		 *           MESSAGES		*
		 *******************************/

:- multifile prolog:message//1.

prolog:message(rdb_load_file(File, Clauses, T)) -->
    [ 'Loaded ~D clauses from ~w in ~3f seconds'-[Clauses, File, T] ].
