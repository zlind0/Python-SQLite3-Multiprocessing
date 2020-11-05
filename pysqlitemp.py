import sqlite3, multiprocessing, os, io
import tabulate, tqdm

# def SimpleTableProcessWrapper(arg):
#     path, tbname, rowids, command=arg
#     pcon=sqlite3.connect(path)
#     stmt=f"SELECT * FROM {tbname} WHERE _ROWID_=?"
#     print(rowids)
#     res=[command(entry) for entry in pcon.executemany(stmt, rowids)]
#     print(res)
#     return res

def MPRowGen(dbpath, stmt):
    pcon=sqlite3.connect(dbpath)
    for entry in pcon.execute(stmt): yield entry

class MPSQLite3Mini:
    def __init__(self, path):
        self.cachestoragepath=path
        self.cachecon=sqlite3.connect(self.cachestoragepath)

    def __setitem__(self, taskname, tuples_iter):
        self.cachecon=sqlite3.connect(self.cachestoragepath)
        first=True
        insertstmt=""
        for entry in tuples_iter:
            if first==True:
                values=",".join(("?" for i in range(0, len(entry))))
                headers=",".join((f"f{i}" for i in range(0, len(entry))))
                self.cachecon.execute(f"DROP TABLE IF EXISTS {taskname}")
                self.cachecon.execute(f"CREATE TABLE {taskname}({headers})")
                insertstmt=f"INSERT INTO {taskname} VALUES({values})"
                self.cachecon.execute(insertstmt, entry)
                break
        self.cachecon.executemany(insertstmt, tuples_iter)
        self.cachecon.commit()

    def __getitem__(self, taskname):
        return self.cachecon.execute(f"SELECT * FROM {taskname}")

    def __delitem__(self, taskname):
        self.cachecon.execute(f"DROP TABLE {taskname}")

class MPSQLite3:
    path=None
    con=None

    def __init__(self, path, tmpstoragepath="_temp.db", cachestoragepath="_cache.db"):
        """
        Declare the file path
        """
        self.path=path
        self.con=sqlite3.connect(self.path)
        self.con.row_factory=sqlite3.Row
        self.tmpstoragepath=tmpstoragepath
        self.cachestoragepath=cachestoragepath
        self.cache=MPSQLite3Mini(cachestoragepath)
        self.con.execute("CREATE TABLE IF NOT EXISTS _KeyValue(key UNIQUE,value)")
        self.con.execute("CREATE INDEX IF NOT EXISTS _IDX_KeyValue ON _KeyValue(key)")
        self.con.execute("CREATE TABLE IF NOT EXISTS _KeyBLOB(key UNIQUE,value BLOB)")
        self.con.execute("CREATE INDEX IF NOT EXISTS _IDX_KeyBLOB ON _KeyValue(key)")
        self.con.execute("ATTACH ? AS TMP", (self.tmpstoragepath,))

    def __del__(self):
        self.con.commit()

    def __setitem__(self, k, v): self.SetKV(k,v)
    def __getitem__(self,k):return self.GetKV(k)
    def __delitem__(self,k):self.DelKV(k)

    def ClearTMP(self):
        cachecon=sqlite3.connect(self.tmpstoragepath)
        tables=[t[0] for t in cachecon.execute('SELECT tbl_name FROM TMP.sqlite_master WHERE TYPE="table"')]
        for t in tables:
            cachecon.execute(f"DROP TABLE TMP.{t}")
        cachecon.commit()
    def ClearCache(self):
        os.remove(self.cachestoragepath)

    def QueryExec(self, stmt, progressbar=False):
        self.con.commit()
        if progressbar==True:
            totstmt=f"SELECT COUNT(1) FROM ({stmt})"
            # print(totstmt)
            for item in self.con.execute(totstmt):
                tot=item[0]
            return tqdm.tqdm(self.con.execute(stmt), total=tot)
        else: return self.con.execute(stmt)

    def QueryPrint(self, stmt, tabulate_tablefmt="orgtbl"):
        self.con.commit()
        cur=self.con.cursor()
        cur.execute(stmt)
        print(tabulate.tabulate(cur, [i[0] for i in cur.description]))

    def PrintTable(self, tablename):
        self.PrintQuery(f"SELECT * FROM {tablename}")

    def SetKV(self, key, value):
        self.con.execute("INSERT OR REPLACE INTO _KeyValue VALUES(?,?)",(key,value))

    def GetKV(self, key):
        self.con.commit()
        for entry in self.con.execute("SELECT value FROM _KeyValue WHERE key=?",(key,)):
            return entry['value']
        return None
    def DelKV(self, key):
        self.con.execute("DELETE FROM _KeyValue WHERE key=?", (key,))

    def SetKBLOB(self, key, BLOB):
        self.con.execute("INSERT OR REPLACE INTO _KeyBLOB VALUES(?,?)",(key,BLOB))
    
    def GetKBLOB(self, key):
        self.con.commit()
        for entry in self.con.execute("SELECT value FROM _KeyBLOB WHERE key=?",(key,)):
            return entry['value']
        return None

    def SetKBLOB_FileHandler(self, key, file):
        self.con.execute("INSERT OR REPLACE INTO _KeyBLOB VALUES(?,?)",(key,file.read()))
    
    def GetKBLOB_FileHandler(self, key):
        self.con.commit()
        for entry in self.con.execute("SELECT value FROM _KeyBLOB WHERE key=?",(key,)):
            return io.BytesIO(entry['value'])
        return None
    
    def SetKBLOB_FilePath(self, key, command=None, remove=False):
        """
        Used for putting file into database after being saved
        Example: 
            model.save("modelname")=>
            SetKBLOB_FilePath("modelname", lambda f: model.save(f), remove=True)
        """
        if command is not None:
            command(key)
        with open(key, "rb") as f:
            self.SetKBLOB_FileHandler(key, f)
        if remove==True:
            os.remove(key)

    def GetKBLOB_FilePath(self, key, command=None, remove=False):
        """
        Used for putting file from database for other commands to read
        Example: 
            model=Glove.load("modelname")=>
            model=GetKBLOB_FilePath("modelname", lambda f: Glove.load(f))
        """
        with open(key, 'wb') as f:
            f.write(self.GetKBLOB(key))
        res=command(key) if command is not None else None
        if remove==True:
            os.remove(key)
        return res

    def DelKBLOB(self, key):
        self.con.execute("DELETE FROM _KeyBLOB WHERE key=?", (key,))

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    

    def SimpleTableProcess(self, tablename, command=None, columns="*", where="", progressbar=True,
                           processes=1, mp_chunk=100, storagepath=None):
        """
        set processes=0 to use all available cores
        """
        self.con.commit()
        if storagepath is None: storagepath=self.path
        stmt=f"SELECT {columns} FROM {tablename} {where}"
        totstmt=f"SELECT COUNT(1) FROM ({stmt})"
        # print(totstmt)
        for item in self.con.execute(totstmt):
            tot=item[0]
        if command is None:
            for item in tqdm.tqdm(MPRowGen(storagepath, stmt), total=tot):
                yield item
        else:
            if processes==0: processes=multiprocessing.cpu_count()
            if processes==1:
                for item in tqdm.tqdm(MPRowGen(storagepath, stmt), total=tot):
                    yield command(item)
            else:
                with multiprocessing.Pool(processes=processes, maxtasksperchild=mp_chunk) as po:
                    res=po.imap(command, MPRowGen(storagepath, stmt), chunksize=mp_chunk)
                    for r in tqdm.tqdm(res,total=tot):
                        yield r

    def ComplexTableProcess(self, select_stmt, command=None, taskname=None, use_cached=False, progressbar=True, processes=1, mp_chunk=100):
        self.con.commit()
        if command is None: taskname="empty_command"
        if taskname is None: taskname=command.__name__
        self.con.execute(f"DROP TABLE IF EXISTS TMP.{taskname}")
        self.con.execute(f"CREATE TABLE TMP.{taskname} AS {select_stmt}")
        self.con.commit()
        for item in self.SimpleTableProcess(taskname, command, progressbar=progressbar, processes=processes,mp_chunk=mp_chunk,
            storagepath=self.tmpstoragepath):
            yield item

    def CacheSave(self, tuples_iter, taskname="empty_task"):
        self.cache[taskname]=tuples_iter

    def CacheLoad(self, taskname="empty_task"):
        return self.cache[taskname]

        
    