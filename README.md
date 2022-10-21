Java version of [robaho/leveldb](https://github.com/robaho/leveldb) - an ultra fast key/value database with Google LevelDB api.

TODO:

sequences are not supported
user defined key comparisons do not work

```
insert time 10000000 records = 42302ms, usec per op 4.2302
close time 1770ms
number of segments 7
scan time 2331ms, usec per op 0.2331
scan time 50% 144ms, usec per op 0.288
random access time 6.476us per get
close with merge 1 time 7123ms
number of segments 1
scan time 1146ms, usec per op 0.1146
scan time 50% 62ms, usec per op 0.124
random access time 5.774us per get
```