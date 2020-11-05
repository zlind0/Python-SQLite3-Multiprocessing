import sqlite3, multiprocessing, os, io
import tabulate

class MPSQLite3:
    path=None
    con=None

    def __init__(self, path):
        """
        Declare the file path
        """
        self.path=path
        self.con=sqlite3.connect(self.path)
        self.con.row_factory=sqlite3.Row
        self.con.execute("CREATE TABLE IF NOT EXISTS _KeyValue(key UNIQUE,value)")
        self.con.execute("CREATE INDEX IF NOT EXISTS _IDX_KeyValue ON _KeyValue(key)")
        self.con.execute("CREATE TABLE IF NOT EXISTS _KeyBLOB(key UNIQUE,value BLOB)")
        self.con.execute("CREATE INDEX IF NOT EXISTS _IDX_KeyBLOB ON _KeyValue(key)")

    def PrintQuery(self, stmt, tabulate_tablefmt="orgtbl"):
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