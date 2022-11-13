Java version of [robaho/leveldb](https://github.com/robaho/leveldb) - an ultra fast key/value database with Google LevelDB api.

requires JDK11+

TODO:

sequences are not supported, but snapshots are
user defined key comparisons do not work

```
write no-sync time 1000000 records = 5599 ms, usec per op 5.599
close time 2127 ms
database size 110.0M
write sync time 10000 records = 473 ms, usec per op 47.300
close time 544 ms
database size 1.1M
batch insert time 1000000 records = 1043 ms, usec per op 1.043
close time 2941 ms
database size 110.0M
write no-sync overwrite time 1000000 records = 4867 ms, usec per op 4.867
close time 1994 ms
database size 219.9M
random read time 7450ms, usec per op 7.45
seq read time 582 ms, usec per op 0.582
compact time 6813 ms
random read time 6671ms, usec per op 6.671
seq read time 162 ms, usec per op 0.162
```