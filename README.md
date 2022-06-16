# Store predicates in RocksDB

This     library     builds     on     top       of     the     [rocksdb
add-on](https://www.swi-prolog.org/pack/list?p=rocksdb).  Right  now  it
requires the GIT version of  SWI-Prolog   and  recompiling the `rocksdb`
pack from source.

The idea of this library is to  see   whether  we can build a persistent
predicate store on top of RocksDB.   The current implementation is there
to access different database organizations to  access how realistic this
is and what kind of performance and scalability is achievable.

## Database organization

The database maps string to Prolog terms.   The  key is a string because
this allows for a  RocksDB  prefix   seek  (using  rocks_enum_from/4) to
enumerate objects.   Keys:

  - ``<PI>\u0001<0-padded hex clause no>``  --> (Head :- Body)
    Key for the Nth clause or _PI_.
  - ``meta\u0001<PI>`` --> `true`
    Key we can enumerate to find defined predicates
  - ``<PI>\u0002<Property>`` --> Term
    Key for properties of _PI_
  - ``<PI>\u0003<Index>`` --> Status
    Key for a clause index created on _PI_
  - ``<PI>\u0004<Index>\u0002<Hash>`` --> list(ClauseRef)
    If arguments related to Index have term_hash/4 Hash, return list
    of candidate clauses.

## First results:

Measured on AMD3950X based system, 64Gb mem and M2 SSD drive.

### Wordnet 3.0

  - 21 files, 21 predicates, 821,492 clauses, 34Mb source text
  - Load time: 11.7 sec.
  - RocksDB size: 99Mb
  - rdb_index(hyp/2, 1) (89,089 clauses) --> 1.35 sec.
  - random query time on hyp(+,-): 10us

### RDF (Geonames)

  - From 1.7Gb HDT file (+ 850Mb index)
  - Load 123,020,820 triples in 2088 sec (CPU)
    - Load performance (clauses/sec) is constant.
  - RocksDB size: 5.2Gb
  - Count triples: 165 sec (152 sec for HDT).
  - rdb_index(rdf/3,1) --> 1383 sec.

## Future

For now, the access predicate is rdb_clause/3.  This is fine for facts
