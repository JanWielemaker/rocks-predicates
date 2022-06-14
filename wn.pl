:- use_module(rocks_preds).

user:file_search_path(wn, '/home/janw/3rdparty/Wordnet-3.0/prolog').

:- initialization
    rdb_open('wn.db').

wn_load :-
    forall(wn_file(File),
           wn_load(File)).

wn_load(File) :-
    rdb_load_file(wn(File)).

wn_file(wn_ant).
wn_file(wn_at).
wn_file(wn_cls).
wn_file(wn_cs).
wn_file(wn_der).
wn_file(wn_ent).
wn_file(wn_fr).
wn_file(wn_g).
wn_file(wn_hyp).
wn_file(wn_ins).
wn_file(wn_mm).
wn_file(wn_mp).
wn_file(wn_ms).
wn_file(wn_per).
wn_file(wn_ppl).
wn_file(wn_sa).
wn_file(wn_sim).
wn_file(wn_sk).
wn_file(wn_s).
wn_file(wn_syntax).
wn_file(wn_vgp).

