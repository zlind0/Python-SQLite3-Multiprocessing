import pysqlitemp,os

instance=pysqlitemp.MPSQLite3("test.db")

print("==================================\nTEST 1: Print table")
instance.QueryPrint("SELECT * FROM avresults LIMIT 10")

print("==================================\nTEST 2: KeyValue")
instance.SetKV("hola","mundo")
print(instance.GetKV("hola"))
print(instance.GetKV("mundo"))
instance.SetKV("hola","world")
print(instance.GetKV("hola"))
print(instance.GetKV("mundo"))
instance.DelKV("hola")
print(instance.GetKV("hola"))
instance["bonjour"]="monde"
print(instance["bonjour"])
print("==================================\nTEST 3: KeyBLOB")
instance.SetKBLOB_FilePath("mahler.mid")
os.remove("mahler.mid")

print(len(instance.GetKBLOB("mahler.mid")))
with instance.GetKBLOB_FileHandler("mahler.mid") as f:
    print(len(f.read()))
instance.GetKBLOB_FilePath("mahler.mid", lambda f: print(os.path.getsize(f)))

print("==================================\nTEST 4: Process Table")
# for _ in instance.SimpleTableProcess("avresults", lambda x: print(tuple(x)), "sha1", "LIMIT 10"):pass
for _ in instance.SimpleTableProcess("avresults", print, "sha1", "LIMIT 10", processes=0):pass
for _ in instance.ComplexTableProcess("SELECT * FROM avclass JOIN avclass2 USING(sha1) LIMIT 10", print):pass

print("==================================\nTEST 5: Cache")
instance.CacheSave(instance.ComplexTableProcess("SELECT * FROM avclass JOIN avclass2 USING(sha1) LIMIT 10"))
for entry in instance.CacheLoad(): print(entry)

instance.cache['avclass']=instance.SimpleTableProcess("avclass",columns="result", where="LIMIT 5")
for i in instance.cache['avclass']: print(i)