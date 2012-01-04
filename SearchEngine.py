#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import gc
import sys
import glob
import time
import array
import cPickle
import cStringIO
import itertools
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
        self.documents_cache_path = os.path.join("cache", "documents.db")
        self.index_cache_path = os.path.join("cache", "index.db")
        self.term_stems_cache_path = os.path.join("cache", "term_stems.db")

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

    def dump_cache(self, filename):
        start = time.time()
        _dict = self.__dict__[os.path.basename(\
                filename.replace(".db", ""))]
        cache_file = open(filename, "wb")
        pickler = cPickle.Pickler(cache_file, protocol=2)
        print "Dumping cache into %s..." % filename
        pickler.dump(_dict)
        cache_file.close()
        print "Dumping of %s finished in "\
              "%.2f seconds." % (filename, time.time() - start)

    def load(self):
        """Load the databases into the engine."""
        print "Loading document and term stems caches from disk..."
        if len(self.documents) == 0:
            print "Loading document cache.."
            self.documents = \
                cPickle.Unpickler(open(self.documents_cache_path, "rb")).load()
        if not self.client and len(self.term_stems) == 0:
            print "Loading term stem cache.."
            self.term_stems = \
                cPickle.Unpickler(open(self.term_stems_cache_path, "rb")).load()
        if self.client and len(self.index) == 0:
            print "Loading inverted index cache.."
            self.index = \
                cPickle.Unpickler(open(self.index_cache_path, "rb")).load()

    def phrasal_query(self, query):
        # Strip quotes, split and stem it
        terms = [STEMMER.stem(_term, 0, len(_term)-1) for \
                _term in query[1:-1].lower().split()]

        results = {}
        for term in terms:
            try:
                results.update(self.index[term])
            except KeyError:
                pass

        # results is a list of dictionary for every term
        # with keys as docno's and values as positions.

        print results
        #docs = set(results.pop().keys())

        return

        for result in results:
            # {'AP890101-0022': '1,2,3,45',
            #  'AP891212-0123': '1,354', for the term 'ahmet' for example.

            # Let's store the intersection of the docno's
            docs.intersection_update(set(result.keys()))

        # Now we have the intersection of documents having the
        # given terms. We now have to find the consecutive appearances.

        for doc in docs:
            #itertools.product(results
            pass


        """
        return dict([docno, self.documents[docno]] \
                for docno in docs.keys()), terms
        """


    def search(self, query):
        if query[0] in  '\'"' and query[-1] in '\'"':
            # Phrasal query
            return self.phrasal_query(query)

        # Plain query
        results = set([])
        terms = query.lower().split()
        intersect = union = False

        searched_terms = []

        for term in terms:
            if term not in ("&&", "||"):
                searched_terms.append(term)

                # result is either empty or a set of docno's
                try:
                    result = set(self.index[term].keys())
                except KeyError:
                    result = set([])

                if result:
                    # The keys are the document numbers
                    if intersect:
                        results.intersection_update(result)
                        intersect = False
                    elif union:
                        results.update(result)
                        union = False
                    else:
                        results.update(result)

            elif term == "&&":
                intersect = True
            elif term == "||":
                union = True

        return dict([docno, self.documents[docno]] \
                for docno in results), searched_terms

    def create_document_cache(self):
        if self.force or not os.path.exists(self.documents_cache_path):
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
            gc.collect()
            self.dump_cache(self.documents_cache_path)

    def create_stem_cache(self):
        if self.force or not os.path.exists(self.term_stems_cache_path):
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
            gc.collect()
            self.dump_cache(self.term_stems_cache_path)

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

        # Then create stem cache
        self.create_stem_cache()

        # Load caches
        self.load()

        # Generate inverted index
        print "Creating inverted index of search terms..."
        start = time.time()

        #pool = multiprocessing.Pool()

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
                        docinfo[docno] = "%s,%s" % (docinfo[docno], ind)
                    except KeyError:
                        docinfo[docno] = "%s" % ind
                except KeyError:
                    self.index[_term] = {docno: "%s" % ind}
                else:
                    del docinfo

        del self.documents
        del self.term_stems

        # Skip stopwords
        self.clean_stop_words()

        print "Index cache (%d terms) created in "\
              "%.2f seconds." % (len(self.index.keys()), time.time() - start)

        # Finally dump index cache
        self.dump_cache(self.index_cache_path)


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
    else:
        engine.load()

    return engine

    #from meliae import scanner
    #scanner.dump_all_objects("searchengine.json")

if __name__ == "__main__":
    engine = main()
