#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import time

from SearchEngine import SearchEngine

if __name__ == "__main__":
    engine = SearchEngine()
    engine.load(client=True)

