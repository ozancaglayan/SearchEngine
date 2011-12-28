#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import gc
import sys
import glob
import time
import cPickle
import multiprocessing

from utils import *

from PorterStemmer import PorterStemmer

class SearchEngine(object):

    def __init__(self):
        self.data_path = "data/AP"
        self.document_cache_path = "cache/documents.db"
        self.index_cache_path = "cache/index.db"

        self.stemmer = PorterStemmer()

    def load(self):
        """Load the databases into the engine."""
        self.document_cache = cPickle.Unpickler(open(self.document_cache_path, "rb")).load()
        self.index_cache = cPickle.Unpickler(open(self.index_cache_path, "rb")).load()

    def search(self, term):
        return self.index_cache.get(term, None)

    def dump_cache(self, _dict, filename):
        cache_file = open(filename, "wb")
        cPickle.Pickler(cache_file, protocol=2)
        cPickle.dump(_dict, cache_file, protocol=2)
        cache_file.close()

    def create_index(self, use_stopwords=True, use_stemmer=False):
        def normalize_term(term, use_stemmer=False):
            """Process the term according some rules and return it."""
            import string

            # First of all convert all cases to lowercase
            nterm = term.lower()

            # Strip out leading and trailing punctuation characters
            nterm = term.strip(string.punctuation)

            # Use porter stemmer to find out the stem of the term
            if use_stemmer:
                nterm = self.stemmer.stem(nterm, 0, len(nterm)-1)

            return nterm

        # Dictionaries
        documents = {}
        index = {}

        # Create a multiprocessing pool
        pool = multiprocessing.Pool(maxtasksperchild=1)

        # List of .Z files to parse/index
        file_list = sorted(glob.glob(os.path.join(self.data_path, "*.Z")))

        # List of stopwords
        stop_words = open("docs/stopwords.txt", "r").read().strip().split("\r\n")

        print "Creating document cache..."
        # Parse every SGML file and generate a document cache
        # for easy retrieval of documents.
        start = time.time()
        for result in pool.map(parse_sgml, file_list, chunksize=10):
            documents.update(result)

        # Cleanup the pool correctly
        pool.close()
        pool.join()

        # Dump document cache
        print "Document cache created in %.2f seconds." % (time.time() - start)
        print "Dumping document cache onto disk..."
        self.dump_cache(documents, self.document_cache_path)
        print "Document cache dumped in %.2f seconds." % (time.time() - start)

        gc.collect()

        # Generate inverted index
        # FIXME: parallelize here
        print "Creating inverted index of search terms..."
        start = time.time()
        """
        pool2 = multiprocessing.Pool()
        keyctr = 0
        for result in pool2.imap(generate_index, documents.iteritems(), chunksize=100):
            docno, terms = result
            keyctr += 1
            print "%d/%d.." % (keyctr, len(documents.keys()))
            for term in terms:
                try:
                    index[term].add(docno)
                except KeyError, ke:
                    index[term] = set([docno])

            del terms

        pool2.close()
        pool2.join()

        """
        for docno, docs in documents.items():
            # Merge sub-documents into one string
            total_docs = "".join(docs)

            # Terms are currently whitespace separated
            for term in total_docs.split():
                normalized_term = normalize_term(term, use_stemmer)
                try:
                    index[normalized_term].add(docno)
                except KeyError, ke:
                    index[normalized_term] = set([docno])

        # Skip stopwords if requested
        skipctr = 0
        if use_stopwords:
            for word in stop_words:
                try:
                    del index[word]
                    skipctr += 1
                except KeyError, ke:
                    pass
        print "%d stopword removed." % skipctr

        print "Index cache (%d terms) created in %.2f seconds." % (len(index.keys()), time.time() - start)

        # Some early cleanup for avoiding memory exhaustion
        print "Cleaning up."
        del documents

        # Dump index cache
        print "Dumping index cache onto disk..."
        start = time.time()
        self.dump_cache(index, self.index_cache_path)
        print "Index cache dumped in %.2f seconds." % (time.time() - start)
        open("keys.txt", "w").write("\n".join(sorted(index.keys())))


# Test the class
def main():
    engine = SearchEngine()
    engine.create_index(use_stemmer=False)


    """
    print "Loading engine."
    engine.load()

    results = engine.search("abandoned")
    if results:
        print "Found %d results matching 'abandoned'." % len(results)
    else:
        print "No results found."
    """


if __name__ == "__main__":
    #import guppy
    #from guppy.heapy import Remote
    #Remote.on()
    main()
