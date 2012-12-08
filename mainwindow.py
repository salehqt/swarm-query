from PySide.QtGui import *
from PySide.QtCore import *
from bsddb3.db import DB
from log import LogRecord


class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        self.setWindowTitle("Swarm Query")
        openAction = QAction("&Open...", self, shortcut = QKeySequence.Open,
                             triggered=self.openFile)
        exitAction = QAction("E&xit", self, shortcut="Ctrl+Q",
                statusTip="Exit the application",
                triggered=self.close)
        fileMenu = self.menuBar().addMenu("&File")
        fileMenu.addAction(openAction)
        fileMenu.addAction(exitAction)

        self.textEdit = QTextEdit(self)
        self.setCentralWidget(self.textEdit)

    def openFile(self):
        fileName, filtr = QFileDialog.getOpenFileName(self, filter="Database files(*.db)")
        if fileName:
            self.loadFile(fileName)

    def loadFile(self,fileName):
        s = ""
        d = DB()
        d.open(fileName)

        c = d.cursor()
        for i in range(1,200):
            r = c.next()
            if(r == None):
                break;
            else:
                s += LogRecord.from_binary(r[1]).bodies_in_keplerian().__str__() + '\n';

        self.textEdit.setText(s)


