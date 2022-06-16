:- use_module(rocks_preds).

user:file_search_path(wn, '/home/janw/3rdparty/Wordnet-3.0/prolog').

:- initialization
    rdb_open('wn.db', _).

wn_load :-
    forall(wn_file(File, _Pred),
           wn_load(File)).

wn_load(File) :-
    rdb_load_file(wn(File)).

wn_file(wn_ant,    ant/4).
wn_file(wn_at,     at/2).
wn_file(wn_cls,    cls/5).
wn_file(wn_cs,     cs/2).
wn_file(wn_der,    der/4).
wn_file(wn_ent,    ent/2).
wn_file(wn_fr,     fr/3).
wn_file(wn_g,      g/2).
wn_file(wn_hyp,    hyp/2).
wn_file(wn_ins,    ins/2).
wn_file(wn_mm,     mm/2).
wn_file(wn_mp,     mp/2).
wn_file(wn_ms,     ms/2).
wn_file(wn_per,    per/4).
wn_file(wn_ppl,    ppl/4).
wn_file(wn_sa,     sa/4).
wn_file(wn_sim,    sim/2).
wn_file(wn_sk,     sk/3).
wn_file(wn_s,      s/6).
wn_file(wn_syntax, syntax/3).
wn_file(wn_vgp,    vgp/4).

index :-
    rdb_index(hyp/2, 1).

hyp(A1,A2) :-
    rdb_clause(hyp(A1,A2), true).

hyp(A1) :-
    hyp(A1, _), !.

thyp :-
    findall(A1, hyp(A1,_), List),
    length(List, N),
    format('~D clauses~n', [N]),
    random_permutation(List, Tests),
    time(maplist(hyp, Tests)).
