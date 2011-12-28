#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
import cStringIO
import gc

def normalize_term(term, use_stemmer=False):
    """Process the term according some rules and return it."""
    import string

    # First of all convert all cases to lowercase
    # Then, strip out leading and trailing punctuation characters
    nterm = term.lower().strip(string.punctuation)

    # Use porter stemmer to find out the stem of the term
    if use_stemmer:
        nterm = self.stemmer.stem(nterm, 0, len(nterm)-1)

    return nterm

def find_all(text, pattern):
    """Returns the indexes of occurence positions."""
    start = 0
    while True:
        start = text.find(pattern, start)
        if start == -1:
            return
        yield start
        start += len(pattern)


def parse_sgml(file_path):
    """Parses a .Z compressed SGML file and return a dict representing the docs."""

    # Result dictionary
    doc_dict = {}

    # First uncompress the file and save the data in uncompressed_data
    proc = subprocess.Popen(["uncompress", "-d", "-c", file_path],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    uncompressed_data = cStringIO.StringIO(proc.communicate()[0])

    # FSM variables
    text_data = ""
    document_no = None
    text = False

    for line in uncompressed_data.readlines():
        line = line.rstrip("\n")
        if line.startswith("</TEXT>"):
            # Now store the document for further caching
            try:
                #printv("Adding %s" % document_no)
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

    uncompressed_data.close()

    # Return the dictionaries
    return doc_dict

def generate_index(document_tuple):

    docno, doctext = document_tuple
    doctext = "".join(doctext)

    # Tuples of (docno,term)
    result = []

    for term in doctext.split():
        #normalized_term = normalize_term(term, False)
        normalized_term = term
        result.append(normalized_term)

    gc.collect()
    return docno, result
