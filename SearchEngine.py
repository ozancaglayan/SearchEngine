#!/usr/bin/python
# -*- coding: utf-8 -*-

"""A simple search engine using inverted indexing."""

import os
import gc
import sys
import glob
import time
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
    """Stems a term and returns unstemmed, stemmed tuple."""
    nterm = term.lower().strip('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
    stem = STEMMER.stem(nterm, 0, len(nterm)-1)
    return (nterm, stem)

def consecutive(sequence):
    """Returns True if the given sequence is consecutive."""
    for i in xrange(len(sequence)-1):
        if int(sequence[i+1]) - int(sequence[i]) != 1:
            return False

    return True

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
    """Main class representing the Search Engine."""
    def __init__(self, force=False, test=False, client=False):
        """Instance initializer."""

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

        # Are the indexes loaded?
        self.is_loaded = False

        # Data structures
        self.documents = {}
        self.index = {}
        self.term_stems = {}
        self.term_set = set([])

    def dump_cache(self, filename):
        """Dumps the relevant dictionary to filename."""
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
        """Loads the databases into the engine."""
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
            self.is_loaded = True

    def phrasal_query(self, query):
        """Does a phrasal query and returns the results."""
        # Strip quotes, split and stem it
        terms = [STEMMER.stem(_term, 0, len(_term)-1) for \
                _term in query.lower().split()]

        results = {}
        for term in terms:
            try:
                results[term] = self.index[term]
            except KeyError:
                # No result found for term, the final result
                # set is empty, we can return!
                return {}, terms

        # results is a dictionary of dictionary
        # with terms as primary key
        # with docno's as secondary key and positions as secondary values
        # Ex: {'ahmet': {'AP890103-0201': '563,784',
        #                'AP890119-0212': '146',
        #                'AP890121-0066': '25',
        #                'AP890123-0121': '175',
        #                'AP890124-0129': '276',
        #                'AP890328-0013': '490',
        #                'AP890513-0022': '537',
        #                'AP890704-0152': '595',
        #                'AP890707-0195': '523',
        #                'AP890715-0134': '35',
        #                'AP890715-0143': '30',
        #                'AP890823-0122': '329',
        #                'AP890830-0260': '30',
        #                'AP891015-0050': '286',
        #                'AP891228-0050': '89'},
        #     'ertegun':{'AP890103-0201': '564',
        #                'AP890513-0022': '538',
        #                'AP890715-0134': '1,26,36,44,134,175,187,195',
        #                'AP890715-0143': '1,26,138,150,158',
        #                'AP890830-0260': '32,36'}
        #           }

        # This is a set of final document set which contains all of the
        # terms(results.keys())
        common_docs = reduce(lambda x, y: set(x).intersection(y), \
                            [pos_dict.keys() for pos_dict \
                            in results.values()])

        # We have to keep the term order in the original query
        # as dicts are unsorted
        final_results = []
        for doc in common_docs:
            # Get the cartesian product of positions for a specific doc
            # In the end we should search for consecutive positions
            # ('35', '1')
            # ('35', '26')
            # ('35', '36') -> Bingo, push the doc to the final result set.
            # Note that the length of the above sequence == len(terms)!
            for product in itertools.product(*[results[term][doc]\
                            .split(",") for term in terms]):
                if consecutive(product):
                    final_results.append(doc)
                    continue

        return dict([docno, self.documents[docno]] \
                for docno in final_results), query.lower().split()

    def search(self, query):
        """Does a simple query and returns the results."""
        if query[0] in  '\'"' and query[-1] in '\'"':
            # Phrasal query
            return self.phrasal_query(query[1:-1])

        # Plain query
        results = set([])
        terms = query.lower().split()
        intersect = union = False

        searched_terms = []

        for term in terms:
            if term not in ("&&", "||"):
                searched_terms.append(term)
                _term = STEMMER.stem(term, 0, len(term)-1)

                # result is either empty or a set of docno's
                try:
                    result = set(self.index[_term].keys())
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
        """Creates the document cache by parsing the .Z files."""
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
        """Creates an intermediate stem cache to use while indexing."""
        if self.force or not os.path.exists(self.term_stems_cache_path):
            print "Creating stem cache for speeding up inverted indexing..."
            pool = multiprocessing.Pool(maxtasksperchild=1000)
            start = time.time()
            for result in pool.map(stem_term, self.term_set, chunksize=10000):
                key, value = result
                self.term_stems[key] = value
            pool.close()
            pool.join()
            print "Created stem cache (%d terms) "\
                  "in %.2f seconds." % (len(self.term_stems), time.time()-start)
            gc.collect()
            self.dump_cache(self.term_stems_cache_path)

    def clean_stop_words(self):
        """Cleans the stop words from the inverted index."""
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
        """Creates the inverted index."""
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

if __name__ == "__main__":
    engine = main()
