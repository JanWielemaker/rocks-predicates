:- module(rocks_preds,
          [ rdb_open/1,                 % +Directory
            rdb_assertz/1,              % +Clause
            rdb_clause/2,               % +Head,-Body
            rdb_clause/3,               % +Head,-Body,-CRef
            rdb_load_file/1,            % +File
            rdb_index/2,                % :Head,+Spec
            rdb_candidates/3            % +Spec,:Head,-Candidates
          ]).
:- use_module(library(rocksdb)).
:- use_module(library(prolog_code)).
:- use_module(library(debug)).
:- use_module(library(filesex)).

/** <module> Store full predicates in a RocksDB

Triples:

  PI.last_clause_id        --> id(Integer)
  PI.id(1..last_clause_id) --> Clause

Interning:
  - int >= 0: as is
  - int <  0: interned term

*/

:- meta_predicate
    rdb_assertz(:),
    rdb_clause(:, -),
    rdb_clause(:, -, -),
    rdb_load_file(:),
    rdb_index(:, +),
    rdb_candidates(+, :, -).


:- dynamic
    intern_table/1,                     % Term --> Id
    extern_table/1,                     % Id --> Term
    triple_table/1,                     % Sid+Pid --> Oid
    index_table/1.                      % Pred+Hash --> list(CRefs)

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

%!  rdb_clause(:Head, -Body) is nondet.
%!  rdb_clause(:Head, -Body, ?CRef) is nondet.
%
%   Retrieve a clause from the persistent database

rdb_clause(Head, Body) :-
    rdb_clause(Head, Body, _).

rdb_clause(Head, Body, CID), nonvar(CID) =>
    extern(CID, Clause),
    Clause = (Head :- Body).
rdb_clause(Head, Body, CID) =>
    pi_head(PI, Head),
    intern(PI, PID),
    intern(last_clause_id, LID),
    t_intern(PID, LID, LastId),
    between(1, LastId, ClauseNo),
    t_intern(PID, ClauseNo, CID),
    extern(CID, Clause),
    Clause = (Head :- Body).

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
		 *           INDEXING		*
		 *******************************/

%!  rdb_index(:Head, +Spec) is det.
%
%   Add an index for the predicate Head.  Spec is one of:
%
%     - An integer
%       Create an index for the Nth argument.

rdb_index(Head, Spec), integer(Spec) =>
    pi_head(PI, Head),
    intern(PI, PID),
    forall(rdb_clause(Head, _, CRef),
           add_to_clause_index(Spec, Head, PID, CRef)).

add_to_clause_index(Spec, _:Head, PID, CRef), integer(Spec) =>
    arg(Spec, Head, Arg),
    term_hash(Arg, 1, 2147483647, Hash),
    s_p_sp(PID, Hash, Id),
    index_table(DB),
    rocks_merge(DB, Id, CRef).

%!  rdb_candidates(+Spec, :Head, -Candidates) is det.

rdb_candidates(Spec, M:Head, Candidates), integer(Spec) =>
    pi_head(PI, M:Head),
    intern(PI, PID),
    arg(Spec, Head, Arg),
    term_hash(Arg, 1, 2147483647, Hash),
    (   var(Hash)
    ->  Candidates = all
    ;   s_p_sp(PID, Hash, Id),
        index_table(DB),
        rocks_get(DB, Id, Candidates)
    ).


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

:- det(intern/2).

intern(Term, Id), integer(Term), Term >= 0 =>
    Id = Term.
intern(Term, Id) =>
    intern_table(DB),
    (   rocks_get(DB, Term, Id0)
    ->  Id = Id0
    ;   extern_table(EDB),
        (   rocks_get(DB, '$$LastID', Id0)
        ->  Id is Id0-1
        ;   Id is -1
        ),
        rocks_put(DB, '$$LastID', Id),
        rocks_put(DB, Term, Id),
        rocks_put(EDB, Id, Term)
    ).

:- det(extern/2).
extern(Id, Term), integer(Id), Id >= 0 =>
    Term = Id.
extern(Id, Term) =>
    extern_table(EDB),
    rocks_get(EDB, Id, Term).

%!  s_p_sp(+S,+P,-SP)
%
%   Combine two interned values to a new one. Note that both S and P can
%   be negative. Prolog cannot cast unsigned   to signed integers, so we
%   represent the pair using a quadruple.
%
%    - 24 bits abs(S)
%    - 38 bits abs(P)
%    -  1 bit  sign(S)
%    -  1 bit  sign(P)

s_p_sp(S,P,SP) :-
    sign(S, P, Sign),
    SP is abs(S)<<40 \/ abs(P)<<2 \/ Sign.

sign(S,P,Sign), S>0, P>0 => Sign = 0x0.
sign(_,P,Sign),      P>0 => Sign = 0x1.
sign(S,_,Sign), S>0      => Sign = 0x2.
sign(_,_,Sign)           => Sign = 0x3.

sp_s_p(SP,S,P) :-
    S0 is SP>>40,
    P0 is SP>>2 /\ 0x1fffffffff,
    (   SP /\ 0x1 =\= 0
    ->  S is -S0
    ;   S = S0
    ),
    (   SP /\ 0x2 =\= 0
    ->  P is -P0
    ;   P = P0
    ).


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
    directory_file_path(Dir, index,  IndexName),
    rocks_open(InternName, IDB, [ key(term), value(int64) ]),
    rocks_open(ExternName, EDB, [ key(int64), value(term) ]),
    rocks_open(TripleName, TDB, [ key(int64), value(int64) ]),
    rocks_open(IndexName,  IDX, [ key(int64), value(term), merge(merge_index) ]),
    asserta(intern_table(IDB)),
    asserta(extern_table(EDB)),
    asserta(triple_table(TDB)),
    asserta(index_table(IDX)).

ensure_directory(Dir) :-
    exists_directory(Dir),
    !.
ensure_directory(Dir) :-
    make_directory(Dir).

:- det(merge_index/5).
merge_index(partial, _Key, Left, Right, Result) :-
    append(Left, Right, Result).
merge_index(full, _Key, Initial, Additions, Result) :-
    append(Initial, Additions, Result).


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
    extern(Sid, S),
    extern(Pid, P),
    extern(Oid, O),
    format('~p ~t~20|~p ~t~40|~p~n', [S,P,O]).

		 /*******************************
		 *           MESSAGES		*
		 *******************************/

:- multifile prolog:message//1.

prolog:message(rdb_load_file(File, Clauses, T)) -->
    [ 'Loaded ~D clauses from ~w in ~3f seconds'-[Clauses, File, T] ].
