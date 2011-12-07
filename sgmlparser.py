#!/usr/bin/python
# -*- coding: utf-8 -*-

import piksemel
from HTMLParser import HTMLParser

class APSGMLParser(HTMLParser):
    def __init__(self, _file):
        HTMLParser.__init__(self)

        self.feed(open(_file, "r").read())

        self._docs = {}

        self.last_tag = None
        self.last_docno = None

    def handle_starttag(self, tag, attrs):
        self.last_tag = tag
    def handle_endtag(self, tag):
        pass
    def handle_data(self, data):
        if self.last_tag == "docno":
            print "'%s'" % data.strip("\n\t ")
            self.last_docno = data.strip()

        if self.last_tag == "text":
            print data
            """
            try:
                self._docs[self.last_docno].append(data)
            except KeyError:
                self._docs[self.last_docno] = [data]
            """

parser = APSGMLParser("AP890101")
