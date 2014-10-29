
import os
import zipfile
from glob import glob

def py_scan(base, rel):
    for a in glob( os.path.join(base, "*") ):
        if a.endswith(".py"):
            yield a, os.path.join(rel, os.path.basename(a))
        elif os.path.isdir(a):
            for a in py_scan(a, os.path.join(rel, os.path.basename(a))):
                yield a

def write_worker_egg(outpath):
    base = os.path.dirname(os.path.abspath(__file__))    
    egg = zipfile.ZipFile(outpath, "w")
    #add the main execution entry point
    egg.write(os.path.join(base, "nebula_executor.py"), "__main__.py")
    
    #add the nebula library
    for f, r in py_scan(base, "nebula"):
        egg.write(f, r)
    
    egg.close()
