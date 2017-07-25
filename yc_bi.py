#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__ = "2017-06-02"
__Author__ = "Tanggang"
__note__ = ""

import multiprocessing
import threading
import Queue
import os
import sys
import time

proj = "yc_bi"


def runAll(classname):
    script_path = os.path.dirname(__file__)
    if script_path != '':
        os.chdir(script_path)
    # modules = files("./"+proj,proj)
    mode = map(__import__, modules)
    index = modules.index(proj + "." + classname)
    t = getattr(mode[index], classname)
    t.table().execute(sys.argv)


def files(path, pre):
    FileList = []
    FileNames = os.listdir(path)
    if (len(FileNames) > 0):
        for fn in FileNames:
            file = os.path.splitext(fn)
            if (file[1] == '.py' and file[0] != "__init__"):
                FileList.append(pre + "." + file[0])
    return FileList


threads = []
classlist = []
modules = files("./" + proj, proj)
for i in modules:
    classlist.append(i.split(".")[1])
for table in classlist:
    threads.append(threading.Thread(target=runAll, args=(table,)))

if __name__ == "__main__":
    if (len(sys.argv)) > 1:
        print sys.argv[1]
        runAll(sys.argv[1])
    else:
        for t in threads:
            print t
            t.setDaemon(True)
            t.start()
        for t in threads:
            t.join()
