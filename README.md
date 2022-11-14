Java version of [robaho/leveldb](https://github.com/robaho/leveldb) - an ultra fast key/value database with Google LevelDB like api.

requires JDK13+.\
Minor mods required to MemoryMapFile to run on earlier JDKs.

TODO:

sequences are not supported, but snapshots are \
user defined key comparisons do not work \
no reverse lookup

DbBench using Oracle JDK13
```
write no-sync time 1000000 records = 5815 ms, usec per op 5.815
close time 2545 ms
database size 109.9M
write sync time 10000 records = 446 ms, usec per op 44.600
close time 565 ms
database size 1.1M
batch insert time 1000000 records = 1098 ms, usec per op 1.098
close time 2741 ms
database size 110.0M
write no-sync overwrite time 1000000 records = 4814 ms, usec per op 4.814
close time 1813 ms
database size 219.9M
random read time 5639ms, usec per op 5.639
seq read time 443 ms, usec per op 0.443
compact time 5376 ms
random read time 5339ms, usec per op 5.339
seq read time 132 ms, usec per op 0.132
```