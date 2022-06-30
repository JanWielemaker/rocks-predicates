:- use_module(rocks_preds).
:- use_module(library(hdt)).
:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(random)).
:- use_module(library(solution_sequences)).
:- use_module(library(statistics)).

% 152 sec; 123,020,820 triples; 1.7+0.8 Gb HDT size
% Load-time: 2088 sec CPU.
% Count triples: 165 sec.
% DB size: 5.2Gb
db(wn, '/home/janw/src/prolog/hdt-data/wordnet.hdt').
db(geonames, '/home/janw/src/prolog/hdt-data/geonames.hdt').

:- dynamic
    hdt/2.

triple(Spec, S,P,O) :-
    db(Spec, File),
    !,
    triple(File, S,P,O).
triple(File, S,P,O) :-
    hdt_handle(File, HDT),
    hdt_search(HDT, S, P, O).

hdt_is_subject(File, S) :-
    triple(File, S,_,_),
    !.


hdt_handle(File, HDT) :-
    (   hdt(File, HDT)
    ->  true
    ;   hdt_open(HDT, File),
        asserta(hdt(File, HDT))
    ).


load(File) :-
    get_time(T0),
    statistics(cputime, CPU0),
    (   State = state(T0, CPU0),
        call_nth(triple(File, S,P,O), N),
        rdb_assertz('rdf.db', rdf(S,P,O)),
        report(N, State),
        fail
    ;   get_time(T1),
        statistics(cputime, CPU1),
        T is T1-T0,
        CPU is CPU1-CPU0,
        print_message(informational, rdf_load(File, T, CPU))
    ).

report(N, State) :-
    N mod 1000 =:= 0,
    !,
    State = state(T0, CPU0),
    get_time(T1),
    statistics(process_cputime, CPU1),
    T is T1-T0,
    CPU is CPU1-CPU0,
    nb_setarg(1, State, T1),
    nb_setarg(2, State, CPU1),
    format(user_error, '\r~t~D~15| ~3f wall, ~3f cpu', [N, T, CPU]),
    (   N mod 1 000 000 =:= 0
    ->  nl(user_error)
    ;   true
    ).
report(_,_).

		 /*******************************
		 *            ACCESS		*
		 *******************************/

rdf(S,P,O) :-
    rdb_clause('rdf.db', rdf(S,P,O), true).

is_subject(S) :-
    rdf(S,_,_),
    !.

random_triple(S,P,O) :-
    rdb_predicate_property('rdf.db', rdf(_,_,_), number_of_clauses(Max)),
    random_between(1, Max, Nth),
    rdb_nth_clause('rdf.db', rdf(S,P,O), Nth, _CRef).

count_triples(Count) :-
    aggregate_all(count, rdf(_S,_P,_O), Count).


access(N) :-
    access(N, time).                    % or `profile`

access(N, How) :-
    findall(S, (between(1,N,_), random_triple(S,_,_)), Subjects0),
    random_permutation(Subjects0, Subjects),
    call(How, maplist(is_subject, Subjects)).

hdt_access(File, N, How) :-
    findall(S, (between(1,N,_), random_triple(S,_,_)), Subjects0),
    random_permutation(Subjects0, Subjects),
    call(How, maplist(hdt_is_subject(File), Subjects)).


:- multifile
    prolog:message//1.

prolog:message(rdf_load(File, T, CPU)) -->
    [ 'Loaded ~w in ~3f sec (~3f sec CPU)'- [File, T, CPU] ].
