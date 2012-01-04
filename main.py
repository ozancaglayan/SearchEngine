#!/usr/bin/python
# -*- coding: utf-8 -*-

from PyQt4 import QtGui
from PyQt4 import QtCore

from ui.ui_mainwindow import Ui_SearchEngineMainWindow

from documentwindow import DocumentWindow

from SearchEngine import SearchEngine

import os

class SearchEngineGUI(QtGui.QDialog, Ui_SearchEngineMainWindow):
    def __init__(self, parent=None):
        QtGui.QDialog.__init__(self, parent)

        self.setupUi(self)

        # Connect buttons
        self.pushButtonLoad.clicked.connect(self.slotLoadIndex)
        self.pushButtonGenerate.clicked.connect(self.slotGenerateIndex)
        self.lineEditQuery.textChanged.connect(self.slotCheckQueryText)
        self.pushButtonSearch.clicked.connect(self.slotProcessQuery)
        self.treeWidgetResults.itemDoubleClicked.connect(self.slotShowDocument)

        # Create search engine instance
        self.engine = SearchEngine(client=True)

    def slotShowDocument(self, item, column):
        documentWindow = DocumentWindow(self, item.text(0),
                                        item.data(1, QtCore.Qt.UserRole+1).toString(),
                                        unicode(item.data(1, QtCore.Qt.UserRole+2).toString()))
        documentWindow.show()

    def slotProcessQuery(self):
        query = self.lineEditQuery.text()
        self.treeWidgetResults.clear()
        result_dict, terms = self.engine.search(unicode(query))
        for docno, docs in result_dict.items():
            record = QtGui.QTreeWidgetItem(self.treeWidgetResults)
            record.setText(0, docno)
            record.setText(1, "%s..." % docs[:250])
            record.setData(1, QtCore.Qt.UserRole+1, docs)
            record.setData(1, QtCore.Qt.UserRole+2, ",".join(terms))

    def slotCheckQueryText(self, text):
        self.pushButtonSearch.setEnabled(self.engine.is_loaded and bool(text))

    def slotGenerateIndex(self):
        self.groupBox.setEnabled(False)
        QtGui.qApp.processEvents()
        self.setWindowTitle("Generating indexes...")
        QtGui.qApp.processEvents()
        QtGui.QApplication.setOverrideCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))

        self.engine.create_index()

        # Restore cursor
        QtGui.QApplication.restoreOverrideCursor()
        QtGui.QApplication.restoreOverrideCursor()
        self.setWindowTitle("Search Engine")
        self.groupBox.setEnabled(True)

    def slotLoadIndex(self):
        self.groupBox.setEnabled(False)
        QtGui.qApp.processEvents()
        self.setWindowTitle("Loading document cache and inverted index...")
        QtGui.qApp.processEvents()
        QtGui.QApplication.setOverrideCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))

        # Load engine
        self.engine.load()

        # Fill UI with some info
        self.labelIndexInfo.setText("Loaded %d documents with %d terms." % (len(self.engine.documents),
                                                                            len(self.engine.index)))
        # Restore cursor
        QtGui.QApplication.restoreOverrideCursor()
        QtGui.QApplication.restoreOverrideCursor()
        self.setWindowTitle("Search Engine")
        self.groupBox.setEnabled(True)

if __name__ == "__main__":
    import sys

    app = QtGui.QApplication(sys.argv)
    QtGui.QApplication.setApplicationName("SearchEngine")

    ser = SearchEngineGUI()
    ser.show()

    app.exec_()
