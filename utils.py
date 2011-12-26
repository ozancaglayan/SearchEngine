#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess

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

