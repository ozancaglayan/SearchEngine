#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
import cStringIO
import gc

def find_all(text, pattern):
    """Returns the indexes of occurence positions."""
    start = 0
    result = []
    while True:
        start = text.find(pattern, start)
        if start == -1:
            return result
        result.append(start)
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
            except KeyError, ke:
                doc_dict[document_no] = "".join(text_data)

            # Reset text_data as we may hit another <Text>
            # with different <Dateline>
            del text_data
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
    del uncompressed_data

    # Return the dictionaries
    return doc_dict, terms
