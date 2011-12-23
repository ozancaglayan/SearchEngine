#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import glob
import cPickle
import subprocess
import multiprocessing

VERBOSE = "-v" in sys.argv

def printv(msg):
    if VERBOSE:
        print msg

def parse_sgml(file_path):
    """Parses a .Z compressed SGML file and return a dict representing the docs."""

    # Result dictionary
    doc_dict = {}

    # First uncompress the file and save the data in uncompressed_data
    proc = subprocess.Popen(["uncompress", "-d", "-c", file_path],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    uncompressed_data = proc.communicate()[0].split("\n")

    # FSM variables
    text_data = ""
    document_no = None
    text = False

    for line in uncompressed_data:
        if line.startswith("</TEXT>"):
            # Now store the document for further caching
            try:
                printv("Adding %s" % document_no)
                doc_dict[document_no].append(text_data)
            except KeyError, ke:
                doc_dict[document_no] = [text_data]
            finally:
                # Reset text_data as we may hit another <Text>
                # with different Dateline
                text_data = ""
                text = False
        elif text:
            # Fetch document text data
            text_data += "%s\n" % line
        elif line.startswith("<DOCNO>"):
            # We've got DOCNO
            document_no = line.replace("<DOCNO>", "").replace("</DOCNO>", "").strip()
        elif line.startswith("<TEXT>"):
            text = True

    # Return the dictionaries
    return doc_dict, index

def generate_index(doc):
        # Fill in the inverted index with the data
        for term in text_data.split():
            try:
                index[term].add(document_no)
            except KeyError, ke:
                index[term] = set([document_no])


class IndexEngine(object):
    """This class parses the .Z documents using the Parser class."""

    def __init__(self):
        self.data_path = "data/AP"
        self.document_cache = "cache/documents.db"

        # List of stopwords
        self.stopwords = open("docs/stopwords.txt", "r").read().strip().split("\n")

        # List of .Z files containing AP documents
        self.file_list = sorted(glob.glob(os.path.join(self.data_path, "*.Z")))

        # Inverted intex
        self.index = {}

        self.documents = {}

    def merge_index(self, new_index):
        for k,v in new_index.items():
            try:
                self.index[k].update(v)
            except KeyError:
                self.index[k] = v

    def start(self):
        # result is a list of parse_sgml()'s return values for
        # every .Z thus it contains 364 result.
        #result = self.pool.map_async(parse_sgml, self.file_list, chunksize=4, callback=parsed_callback)
        #result.wait()
        pool = multiprocessing.Pool(maxtasksperchild=1)

        for result in pool.imap_unordered(parse_sgml, self.file_list, chunksize=10):
            self.documents.update(result[0])

        _f = open(self.document_cache, "wb")
        cPickle.Pickler(_f, protocol=2)
        cPickle.dump(self.documents, f, protocol=2)

        del self.documents
        import time
        print "Sleeping.."
        time.sleep(5)
        pool.close()
        pool.join()
        time.sleep(5)


    def index(self):
        print self.index

    def create_index(self):
        """Creates a document index in the index_dir directory."""
        pass


if __name__ == "__main__":
    engine = IndexEngine()
    engine.start()


