#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import gc
import sys
import glob
import time
import string
import cPickle
import multiprocessing

from utils import *
from PorterStemmer import PorterStemmer

class SearchEngine(object):

    def __init__(self):
        self.data_path = "data/AP"
        self.document_cache_path = "cache/documents.db"
        self.index_cache_path = "cache/index.db"
        self.stem_cache_path = "cache/stems.db"

        self.document_cache = False
        self.index_cache = False

        self.stemmer = PorterStemmer()

    def load(self):
        """Load the databases into the engine."""
        self.document_cache = cPickle.Unpickler(open(self.document_cache_path, "rb")).load()
        self.index_cache = cPickle.Unpickler(open(self.index_cache_path, "rb")).load()

    def is_loaded(self):
        return self.document_cache and \
               self.index_cache

    def get_document(self, docid):
        return self.document_cache.get(docid, "")

    def search(self, query):
        results = set([])
        terms = query.split()
        intersect = union = False

        for term in terms:
            if term not in ("&&", "||"):
                if intersect:
                    results = results.intersection(self.index_cache.get(term, set([])))
                    intersect = False
                elif union:
                    results = results.union(self.index_cache.get(term, set([])))
                    union = False
                else:
                    results.update(self.index_cache.get(term, set([])))
            elif term == "&&":
                intersect = True
            elif term == "||":
                union = True

        return dict([docno, self.document_cache[docno]] for docno in results)

    def dump_cache(self, _dict, filename):
        cache_file = open(filename, "wb")
        cPickle.Pickler(cache_file, protocol=2)
        cPickle.dump(_dict, cache_file, protocol=2)
        cache_file.close()

    def create_index(self, force=False):
        # Dictionaries & sets
        documents = {}
        index = {}
        term_stems = {}
        term_set = set([])

        # List of stopwords
        stop_words = open("docs/stopwords.txt", "r").read().strip().split("\r\n")

        # Make them local references
        punctuation = string.punctuation
        digits = "$%s" % string.digits

        if force or not os.path.exists(self.document_cache_path):
            # Create a multiprocessing pool
            pool = multiprocessing.Pool(maxtasksperchild=1)

            # List of .Z files to parse/index
            file_list = sorted(glob.glob(os.path.join(self.data_path, "*.Z")))

            print "Creating document cache..."
            # Parse every SGML file and generate a document cache
            # for easy retrieval of documents.
            start = time.time()
            for result in pool.map(parse_sgml, file_list, chunksize=10):
                doc_dict, terms = result
                documents.update(doc_dict)
                term_set.update(terms)

            # Cleanup the pool correctly
            pool.close()
            pool.join()
            print "Document cache created in %.2f seconds." % (time.time() - start)

            print "Creating stem dictionary for speeding up inverted indexing..."
            start = time.time()
            for term in term_set:
                nterm = term.lower().strip(punctuation)
                term_stems[nterm] = self.stemmer.stem(nterm, 0, len(nterm)-1)
            print "created stem dictionary (%d terms) in %.2f seconds." % (len(term_stems), time.time()-start)

            # Dump caches
            print "Dumping document cache onto disk..."
            start = time.time()
            self.dump_cache(documents, self.document_cache_path)
            print "Document cache dumped in %.2f seconds." % (time.time() - start)
            print "Dumping stem cache onto disk..."
            start = time.time()
            self.dump_cache(term_stems, self.stem_cache_path)
            print "Stem cache dumped in %.2f seconds." % (time.time() - start)

            gc.collect()
        else:
            print "Loading document and term stems caches from disk..."
            documents = cPickle.Unpickler(open(self.document_cache_path, "rb")).load()
            term_stems = cPickle.Unpickler(open(self.stem_cache_path, "rb")).load()

        # Generate inverted index
        print "Creating inverted index of search terms..."
        start = time.time()

        for docno, docs in documents.items():
            # Merge sub-documents into one string
            total_docs = "".join(docs)

            # Terms are whitespace delimited
            stripped_terms = [t.strip(punctuation) for t in total_docs.split()]
            terms = [_term.lower() for _term in stripped_terms \
                    if _term and _term[0] not in digits]
            #len_terms = float(len(terms))
            for term in terms:
                #positions = find_all(total_docs, term)

                # Add the term to the inverted index
                _term = term_stems[term]
                #freq = total_docs.count(_term)/len_terms
                try:
                    #index[_term].add((docno, freq))
                    index[_term].add(docno)
                    #index[_term].add(docno)
                except KeyError, ke:
                    #index[_term] = set([(docno, freq)])
                    index[_term] = set([docno])
                    #index[_term] = set([docno])

        print "Index cache (%d terms) created in %.2f seconds." % (len(index.keys()), time.time() - start)

        # Skip stopwords
        skipctr = 0
        for word in stop_words:
            try:
                del index[word]
                skipctr += 1
            except KeyError, ke:
                pass
        print "%d stopword(s) removed." % skipctr

        # Some early cleanup for avoiding memory exhaustion
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
    engine.create_index()

if __name__ == "__main__":
    main()
