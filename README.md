# K/V store based predicates

## Head representation

  - Pred -> Pred-no
  - Clause-No --> Clause
  - Indexes
    - Pred-no+arg --> list of clauses

Or, pred/db?

## RocksDB extensions

  - Integer pairs as keys (extend to N-pairs?)
    - rocks_open(Dir, DB, [key(2*int64), ...]),
    - rockdb_put(+DB, v(+S, +P), +O)
      Where S and P are 64 bit integers
    - rockdb_get(+DB, v(+S, +P), -O)
      Where S and P are 64 bit integers
    - rockdb_enum(+DB, v(-S,-P), -O)

  - Lists of integers as value
    - rocks_open(Dir, DB, [value(list(int64)), ...])
    - rocks_open(Dir, DB, [value(set(int64)), ...])
      - Adds merge function
      - Retrieves as Prolog list

### Blog

  - https://jiaweichiu.github.io/articles/2018-12/rocksdb-merge-operators
  - https://github.com/facebook/rocksdb/wiki/Manual-Compaction
