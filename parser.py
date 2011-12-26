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

            # Reset text_data as we may hit another <Text>
            # with different Dateline
            text_data = ""
            text = False
        elif text:
            # Fetch document text data
            text_data += "%s\n" % line
        elif line.startswith("<DOCNO>"):
            # We've got DOCNO
            document_no = line.split()[1]
        elif line.startswith("<TEXT>"):
            text = True

    del uncompressed_data

    # Return the dictionaries
    return doc_dict

def generate_index(doc):
        # Fill in the inverted index with the data
        index = {}
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
        self.index_cache = "cache/index.db"

        # List of stopwords
        self.stopwords = open("docs/stopwords.txt", "r").read().strip().split("\n")

        # List of .Z files containing AP documents
        self.file_list = sorted(glob.glob(os.path.join(self.data_path, "*.Z")))

    def start(self):
        documents = {}
        index = {}

        # Create a multiprocessing pool
        pool = multiprocessing.Pool(maxtasksperchild=1)

        # Parse every SGML file and generate a document cache
        # for easy retrieval of documents.
        for result in pool.map(parse_sgml, self.file_list, chunksize=10):
            documents.update(result)

        # Dump document cache
        cache_file = open(self.document_cache, "wb")
        cPickle.Pickler(cache_file, protocol=2)
        cPickle.dump(documents, cache_file, protocol=2)
        cache_file.close()

        # Generate inverted index
        for docno, docs in documents.items():
            for doc in docs:
                # Terms are currently whitespace separated
                for term in doc.split():
                    try:
                        index[term].add(docno)
                    except KeyError, ke:
                        index[term] = set([docno])

        # Some early cleanup for avoiding memory exhaustion
        del documents

        # Dump index cache
        cache_file = open(self.index_cache, "wb")
        cPickle.Pickler(cache_file, protocol=2)
        cPickle.dump(index, cache_file, protocol=2)
        cache_file.close()

if __name__ == "__main__":
    engine = IndexEngine()
    engine.start()


