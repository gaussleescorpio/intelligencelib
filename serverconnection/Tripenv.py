

import ServerConnectionlocal1

import sys
from PyQt5.QtWidgets import QApplication, QWidget, QCheckBox, QVBoxLayout,QHBoxLayout



class Data_box(QWidget):
    def __init__(self):
        super(QWidget,self).__init__()
        self.Init_UI()

    def Init_UI(self):
        self.resize(250,300)
        self.__Hboxlayout =QHBoxLayout()
        self.__subVboxlayout = QVBoxLayout()
        
        self.show()




if __name__ == '__main__':
    libpaths = QApplication.libraryPaths()
    if sys.platform == 'darwin':
        libpaths.append("/Users/gausslee/Documents/Qt5.5/5.5/clang_64/plugins")
        QApplication.setLibraryPaths(libpaths)
    elif sys.platform == 'win32':
        libpaths.append("C:/Users/gausslee/Anaconda/envs/py34/Lib/site-packages/PyQt5/plugins")
        QApplication.setLibraryPaths(libpaths)

    app = QApplication(sys.argv)

    # w = QWidget()
    # w.resize(250, 150)
    # w.move(300, 300)
    # w.setWindowTitle('Simple')
    # w.show()
    ex = Data_box()

    sys.exit(app.exec_())



