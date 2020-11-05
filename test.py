import pysqlitemp,os

sq=pysqlitemp.MPSQLite3("test.db")

print("==================================\nTEST 1: Print table")
sq.QueryPrint("SELECT * FROM avresults LIMIT 10")

print("==================================\nTEST 2: KeyValue")
sq.SetKV("hola","mundo")
print(sq.GetKV("hola"))
print(sq.GetKV("mundo"))
sq.SetKV("hola","world")
print(sq.GetKV("hola"))
print(sq.GetKV("mundo"))
sq.DelKV("hola")
print(sq.GetKV("hola"))
sq["bonjour"]="monde"
print(sq["bonjour"])
print("==================================\nTEST 3: KeyBLOB")
sq.SetKBLOB_FilePath("mahler.mid")
os.remove("mahler.mid")

print(len(sq.GetKBLOB("mahler.mid")))
with sq.GetKBLOB_FileHandler("mahler.mid") as f:
    print(len(f.read()))
sq.GetKBLOB_FilePath("mahler.mid", lambda f: print(os.path.getsize(f)))

print("==================================\nTEST 4: Process Table")
# for _ in sq.SimpleTableProcess("avresults", lambda x: print(tuple(x)), "sha1", "LIMIT 10"):pass
for _ in sq.SimpleTableProcess("avresults", print, "sha1", "LIMIT 10", processes=0):pass
for _ in sq.ComplexTableProcess("SELECT * FROM avclass JOIN avclass2 USING(sha1) LIMIT 10", print):pass

print("==================================\nTEST 5: Cache")
sq.CacheSave(sq.ComplexTableProcess("SELECT * FROM avclass JOIN avclass2 USING(sha1) LIMIT 10"))
for entry in sq.CacheLoad(): print(entry)

sq.cache['avclass']=sq.SimpleTableProcess("avclass",columns="result", where="LIMIT 5")
for i in sq.cache['avclass']: print(i)