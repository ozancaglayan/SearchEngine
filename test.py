#!/usr/bin/python
# -*- coding: utf-8 -*-

import cPickle

from PorterStemmer import PorterStemmer

d = cPickle.Unpickler(open("cache/documents.db", "rb")).load()

porter = PorterStemmer()

dic = {}

for doc in d.values():
    total = "".join(doc)

    for term in total.split():
        if ternot dic[term]:
            dic[term] = porter.stem(term, 0, len(term)-1)
