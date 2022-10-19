Java version of [robaho/leveldb](https://github.com/robaho/leveldb) - an ultra fast key/value database with Google LevelDB api.

TODO:

sequences are not supported
user defined key comparisons do not work

```
insert time 10000000 records = 15443ms, usec per op 1.5443
close time 4954ms
scan time 1934ms, usec per op 0.1934
scan time 50% 81ms, usec per op 0.162
random access time 6.264us per get
close with merge 1 time 0ms
scan time 2077ms, usec per op 0.2077
scan time 50% 67ms, usec per op 0.134
random access time 6.083us per get
```