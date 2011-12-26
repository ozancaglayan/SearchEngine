#!/usr/bin/python
# -*- coding: utf-8 -*-

from SearchEngine import SearchEngine

if __name__ == "__main__":
    engine = SearchEngine()
    engine.load()

    engine.search(sys.argv[1])
