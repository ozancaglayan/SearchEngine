#!/usr/bin/python
# -*- coding: utf-8 -*-

from PyQt4 import QtGui
from PyQt4 import QtCore

from ui.ui_documentwindow import Ui_DocumentWindow

from string import punctuation

from PorterStemmer import PorterStemmer

class DocumentWindow(QtGui.QDialog, Ui_DocumentWindow):
    def __init__(self, parent, docno, doc, terms):
        QtGui.QDialog.__init__(self, parent)

        self.setupUi(self)

        # Set fields
        self.labelDocumentNo.setText(docno)

        textDocument = self.textEdit.document()
        textCursor = QtGui.QTextCursor(textDocument)

        normalFormat = QtGui.QTextCharFormat()
        termFormat = QtGui.QTextCharFormat()
        termFormat.setForeground(QtGui.QBrush(QtGui.QColor("red")))
        termFormat.setFontWeight(QtGui.QFont.Bold)

        textCursor.beginEditBlock()

        stemmer = PorterStemmer()
        terms = terms.split(",")
        stemmed_terms = [stemmer.stem(term, 0, len(term)-1) for term in terms]

        for line in unicode(doc).split("\n"):
            for word in line.split(" "):
                nword = word.lower().strip(punctuation)
                sword = stemmer.stem(nword, 0, len(nword)-1)
                if nword in terms or sword in stemmed_terms:
                    textCursor.insertText(word, termFormat)
                else:
                    textCursor.insertText(word, normalFormat)
                textCursor.insertText(" ", normalFormat)

            textCursor.insertText("\n", normalFormat)

        self.textEdit.moveCursor(QtGui.QTextCursor.Start)
