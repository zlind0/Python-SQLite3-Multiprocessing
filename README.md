# Python SQLite Utils Optimized for Multi-Processing

## Key features:

1. Parallel read and processing
2. Delayed write&update
3. Python map to sqlite table (best for unexpected field)
4. Key-Value storage
5. Key-BLOB(BLOB as file handler) storage
6. Tabulate query display
7. Python list wrapper
8. Python map wrapper

## Usage:

0. Initialization

```
sq=pysqlitemp.MPSQLite3("path_to_your_db.db", 
    tmpstoragepath="_temp.db", 
    cachestoragepath="_cache.db")
```
`tmpstoragepath` is a cache db file for some complex query. When you need parallel processing, we may also write processed data to disk. Cache can prevent read and write at the same time.
`cachestoragepath` is a cache db to save results when iterating `tmpstoragepath` for parallel processing.

1. Parallel read and processing

Simply process a table. This method was designed for you to create
```
for result in sq.SimpleTableProcess("avclass",columns="result", where="LIMIT 5", processes=0):
    process(result)
```
`process=0` doesn't indicate single-process, instead, it indicates to use all available cores.

2. Delayed write&update

```
sq.cache['avclass']=sq.SimpleTableProcess("avclass",columns="result", where="LIMIT 5", processes=0)
```
This stores all results into cache database. Then you can read cache:
```
for i in sq.cache['avclass']: print(i)
```

3. Python map to sqlite table (best for unexpected field)

Insert a map into table.
```
sq.InsertMap(map, tablename)
sq.InsertMap({"hola":"mundo", "hello":"world"}, "hola")
sq.InsertMap({"bonjour":"lemonde"}, "hola")
```


4. Key-Value storage
As simple as you can imagine.
```
sq["bonjour"]="monde"
print(sq["bonjour"])
```

5. Key-BLOB(BLOB as file handler) storage

Reads a file into BLOB
```
sq.SetKBLOB_FilePath("mahler.mid")
```
Reads a BLOB into bytes:
```
print(len(sq.GetKBLOB("mahler.mid")))
```
Opens a BLOB as file handler:
```
with sq.GetKBLOB_FileHandler("mahler.mid") as f:
    print(len(f.read()))
```
Opens a BLOB and save as temporary file for some functions that only accepts path.
```
sq.GetKBLOB_FilePath("mahler.mid", lambda f: print(os.path.getsize(f)))
```
6. Tabulate query display
```
sq.QueryPrint("SELECT * FROM avresults LIMIT 10")
```
7. Query with Progressbar

On con.execute, progress is triggered when an item is fetched.
```
    for i in sq.QueryExec("SELECT * FROM avclass JOIN avclass2 USING(sha1) LIMIT 10"): print(tuple(i))
```

On con.executemany, progress is triggered when the iterable yields an item.
```
    sq.QueryExecMany("INSERT INTO test7 VALUES(?)", (("1"), ("2")))
```

When creating table, you should turn off the progressbar.
```
    sq.QueryExec("CREATE TABLE IF NOT EXISTS test7(val)", progressbar=False)
```