#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import gc
import sys
import glob
import time
import cPickle
import cStringIO
import subprocess
import multiprocessing

from PorterStemmer import PorterStemmer

STEMMER = PorterStemmer()

#############
# Utilities #
#############
def stem_term(term):
    nterm = term.lower().strip('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
    stem = STEMMER.stem(nterm, 0, len(nterm)-1)
    return (nterm, stem)

def dump_cache(_dict, filename):
    start = time.time()
    cache_file = open(filename, "wb")
    cPickle.Pickler(cache_file, protocol=2)
    print "Dumping cache into %s..." % filename
    cPickle.dump(_dict, cache_file, protocol=2)
    cache_file.close()
    print "Dumping of %s finished in "\
          "%.2f seconds." % (filename, time.time() - start)

def parse_sgml(file_path):
    """Parses a .Z compressed SGML file and returns
    a dict representing the docs."""

    # Result dictionary
    doc_dict = {}

    # First uncompress the file and save the data in uncompressed_data
    proc = subprocess.Popen(["uncompress", "-d", "-c", file_path],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    uncompressed_data = cStringIO.StringIO(proc.communicate()[0])

    # FSM variables
    text_data = []
    terms = set([])
    document_no = None
    text = False

    for line in uncompressed_data.readlines():
        line = line.rstrip("\n")
        if line.startswith("</TEXT>"):
            # Now store the document for further caching
            try:
                doc_dict[document_no] += "".join(text_data)
            except KeyError:
                doc_dict[document_no] = "".join(text_data)

            # Reset text_data as we may hit another <Text>
            # with different <Dateline>
            text_data = []
            text = False
        elif text:
            # Fetch document text data
            text_data.append("%s\n" % line)
            terms.update(line.split())
        elif line.startswith("<DOCNO>"):
            # We've got DOCNO
            document_no = line.split()[1]
        elif line.startswith("<TEXT>"):
            text = True

    uncompressed_data.close()

    # Return the dictionaries
    return doc_dict, terms



###########################
# Main SearchEngine class #
###########################

class SearchEngine(object):
    def __init__(self, force=False, test=False, client=False):
        # Paths
        self.data_path = os.path.join("data", "AP")
        self.document_cache_path = os.path.join("cache", "documents.db")
        self.index_cache_path = os.path.join("cache", "index.db")
        self.stem_cache_path = os.path.join("cache", "stems.db")

        # Force creation of document and stem caches
        self.force = force

        # Reduce document base' size for quick testing
        self.test = test

        # Only load the document and index caches
        self.client = client

        # Data structures
        self.documents = {}
        self.index = {}
        self.term_stems = {}
        self.term_set = set([])

    def load(self):
        """Load the databases into the engine."""
        print "Loading document and term stems caches from disk..."
        if len(self.documents) == 0:
            self.documents = \
                cPickle.Unpickler(open(self.document_cache_path, "rb")).load()
        if not self.client and len(self.term_stems) == 0:
            self.term_stems = \
                cPickle.Unpickler(open(self.stem_cache_path, "rb")).load()
        if self.client and len(self.index) == 0:
            self.index = \
                cPickle.Unpickler(open(self.index_cache_path, "rb")).load()

    def search(self, query):
        results = set([])
        terms = query.lower().split()
        intersect = union = False

        searched_terms = []

        for term in terms:
            if term not in ("&&", "||"):
                searched_terms.append(term)
                if intersect:
                    results = results.intersection(\
                            self.index.get(term, set([])))
                    intersect = False
                elif union:
                    results = results.union(self.index.get(term, set([])))
                    union = False
                else:
                    results.update(self.index.get(term, set([])))
            elif term == "&&":
                intersect = True
            elif term == "||":
                union = True

        return dict([docno, self.documents[docno]] \
                for docno in results), searched_terms

    def create_document_cache(self):
        if self.force or not os.path.exists(self.document_cache_path):
            # Create a multiprocessing pool
            pool = multiprocessing.Pool(maxtasksperchild=1)

            # List of .Z files to parse/index
            file_list = sorted(glob.glob(os.path.join(self.data_path, "*.Z")))

            if self.test:
                # Reduce the data set
                file_list = file_list[:100]

            print "Creating document cache..."
            # Parse every SGML file and generate a document cache
            # for easy retrieval of documents.
            start = time.time()
            for result in pool.map(parse_sgml, file_list, chunksize=50):
                doc_dict, terms = result
                self.documents.update(doc_dict)
                self.term_set.update(terms)

            # Cleanup the pool correctly
            pool.close()
            pool.join()
            print "Document cache created in "\
                    "%.2f seconds." % (time.time() - start)

    def create_stem_cache(self):
        if self.force or not os.path.exists(self.stem_cache_path):
            print "Creating stem dictionary for speeding up inverted indexing..."
            pool = multiprocessing.Pool(maxtasksperchild=1000)
            start = time.time()
            for result in pool.map(stem_term, self.term_set, chunksize=10000):
                key, value = result
                self.term_stems[key] = value
            pool.close()
            pool.join()
            print "Created stem dictionary (%d terms) "\
                  "in %.2f seconds." % (len(self.term_stems), time.time()-start)

    def clean_stop_words(self):
        skipctr = 0
        # List of stopwords
        stop_words = open(os.path.join("docs", "stopwords.txt"), \
                "r").read().strip().split("\r\n")

        for word in stop_words:
            try:
                del self.index[word]
                skipctr += 1
            except KeyError:
                pass
        print "%d stopword(s) removed." % skipctr

    def create_index(self):
        # Make them local references for speeding up
        punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
        digits = "$0123456789"

        # First create document cache
        self.create_document_cache()

        gc.collect()

        # Then create stem cache
        self.create_stem_cache()

        gc.collect()

        # Dump caches
        dump_cache(self.documents, self.document_cache_path)
        dump_cache(self.term_stems, self.stem_cache_path)

        # Load caches
        self.load()

        gc.collect()

        # Generate inverted index
        print "Creating inverted index of search terms..."
        start = time.time()

        for docno, doc in self.documents.iteritems():
            # Terms are whitespace delimited
            stripped_terms = [t.strip(punctuation) for t in doc.split()]
            terms = [_term.lower() for _term in stripped_terms \
                    if _term and _term[0] not in digits]
            for ind, term in enumerate(terms):
                # Add the term's to the inverted index
                _term = self.term_stems[term]
                try:
                    docinfo = self.index[_term]
                    try:
                        docinfo[docno].append(ind)
                    except KeyError:
                        docinfo[docno] = [ind]
                    #index[_term].add(docno)
                except KeyError:
                    self.index[_term] = {docno: [ind]}
                    #index[_term] = set([docno])

        # Skip stopwords
        self.clean_stop_words()

        print "Index cache (%d terms) created in "\
              "%.2f seconds." % (len(self.index.keys()), time.time() - start)

        # Finally dump index cache
        dump_cache(self.index, self.index_cache_path)
        #open("keys.txt", "w").write("\n".join(sorted(index.keys())))


# Test the class
def main():
    force = False
    test = False
    client = False
    if "--test" in sys.argv:
        test = True
    if "--force" in sys.argv:
        force = True
    if "--client" in sys.argv:
        client = True

    engine = SearchEngine(force=force, test=test, client=client)

    if not client:
        engine.create_index()

    #from meliae import scanner
    #scanner.dump_all_objects("searchengine.json")

if __name__ == "__main__":
    main()
