#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import glob

for ui in glob.glob1("ui", "*.ui"):
    os.system("/usr/bin/pyuic4 -o ui/ui_%s.py ui/%s -g searchengine" % (ui.split(".")[0], ui))

try:
    if sys.argv[1] == "-x":
        os.system("./main.py")
except:
    pass
