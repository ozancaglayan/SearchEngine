#!/usr/bin/python
# -*- coding: utf-8 -*-

from PyQt4 import QtGui
from PyQt4 import QtCore

from ui.ui_mainwindow import Ui_SearchEngineMainWindow

import os

class SearchEngineGUI(QtGui.QDialog, Ui_SearchEngineMainWindow):
    def __init__(self, parent=None):
        QtGui.QDialog.__init__(self, parent)

        self.setupUi(self)

    """
    def slotShowTrainingDialog(self):
        self.groupBox.setEnabled(False)
        QtGui.qApp.processEvents()
        self.setWindowTitle("Training...")
        QtGui.qApp.processEvents()
        QtGui.QApplication.setOverrideCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))

        # Train
        self.marf.write_speakers()
        ret = self.marf.train()

        # Restore cursor
        QtGui.QApplication.restoreOverrideCursor()
        QtGui.QApplication.restoreOverrideCursor()
        self.setWindowTitle("Speaker Identification")
        self.groupBox.setEnabled(True)
    """

if __name__ == "__main__":
    import sys

    app = QtGui.QApplication(sys.argv)
    QtGui.QApplication.setApplicationName("SearchEngine")

    ser = SearchEngineGUI()
    ser.show()

    app.exec_()
