from GetTripEnvAdvanced import AdvanceDataFetcher

import sys
from PyQt5.QtWidgets import QApplication, QWidget, QCheckBox, QVBoxLayout,QHBoxLayout, QCalendarWidget, QGridLayout
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import PyQt5.QtWidgets as Qt5


import numpy as np

# derived class from CheckBox with more signals and slots
class CheckBox(Qt5.QCheckBox):
    stateChangedstr = pyqtSignal(Qt5.QCheckBox)
    toggleChanged = pyqtSignal()

    def __init__(self,*args):
        super(CheckBox, self).__init__(*args)
        self.stateChanged.connect(self.on_state)
        self.toggleChanged.connect(self.on_togglechange)

    def on_state(self):
        self.stateChangedstr.emit(self)

    def on_togglechange(self):
        if self.isChecked():
            self.setChecked(False)
        else:
            self.setChecked(True)
        print "set %s"%self.text + "...."*9


# generic worker for multi-threading
class GenericWorker(QObject):
    def __init__(self, function, *args, **kwargs):
        super(GenericWorker, self).__init__()

        self.ithread = QThread()
        self.moveToThread(self.ithread)
        self.ithread.finished.connect(self.deleteLater)
        self.ithread.finished.connect(self.ithread.deleteLater)
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.start.connect(self.run)
        self.terminate.connect(self.terminate_thread)
        self.ithread.start()



    start = pyqtSignal(str)
    terminate = pyqtSignal()

    @pyqtSlot()
    def run(self):
        self.function(*self.args, **self.kwargs)
        self.terminate.emit()
        self.ithread.finished.emit()



    def terminate_thread(self):
        # print "thread terminating...."
        self.ithread.terminate()
        # self.ithread.wait()



class TripEnvGui(QWidget):
    chain_event = pyqtSignal(QCheckBox)

    def __init__(self):
        QWidget.__init__(self)
        self.fetcher = AdvanceDataFetcher(host_name="bisam-client01", user_name="gauss", pass_word="gauss")
        self.chain_event.connect(self.chain_handler)
        self.init_gui()
        self.show()

    def init_gui(self):
        # self.resize(200,250)
        self.__attr_list = self.fetcher.get_attr_list()
        # init all the layouts for different parts of GUI
        self.H_Layout = QHBoxLayout()

        self.L_VLayout = QVBoxLayout()
        self.R_VLayout = QVBoxLayout()
        self.UpLeftLayout = QHBoxLayout()
        self.MdLeftLayout = QGridLayout()
        self.DnLeftLayout = QHBoxLayout()
        # Layout for left top side
        self.__group_filter = Qt5.QGroupBox("Filter setting:")
        self.__group_filter.setFixedHeight(100)
        self.__start_cal = Qt5.QDateEdit()
        self.__end_cal = Qt5.QDateEdit()
        self.__start_cal.setCalendarPopup(True)
        self.__end_cal.setCalendarPopup(True)
        self.UpLeftLayout.addWidget(self.__start_cal)
        self.UpLeftLayout.addWidget(self.__end_cal)
        self.__group_filter.setLayout(self.UpLeftLayout)


        # Layout for left middle side
        self.__group_feature = Qt5.QGroupBox("Feature selection:")
        num_of_features = 0
        self.__feature_groups = {}
        self.__feature_group_layout = {}
        self.__checkbox_group = {}
        self.__checkbox_group_names={}
        self.__checkbox_group_keys ={}
        self.__selected_features = {}
        for key in self.__attr_list["ibemining"]["alias"].values():
            new_group = Qt5.QGroupBox(key)
            new_layout = Qt5.QVBoxLayout()
            new_group.setLayout(new_layout)
            self.__feature_group_layout[key] = new_layout
            self.__feature_groups[key] = new_group
            self.__selected_features = []
        for i,key in enumerate( self.__feature_groups.keys() ):
            if key == "r_countries":
                continue

            self.__checkbox_group[key] = []
            self.__checkbox_group_names[key] = []
            self.__checkbox_group_keys[key] = []
            for attr, key_n in zip( self.__attr_list["ibemining"][key].values(), \
                                self.__attr_list["ibemining"][key].keys() ):
                if key_n == "tb_alias":
                    continue
                check_box = CheckBox(attr)

                check_box.stateChangedstr.connect(self.update_selected)
                self.__checkbox_group[key].append(check_box)
                self.__checkbox_group_names[key].append(attr)
                self.__checkbox_group_keys[key].append(key_n)
                self.__feature_group_layout[key].addWidget(check_box)
            # self.__feature_groups[key].setLayout(self.__feature_group_layout[key])
            if key == "d_air_booking":
                x_pos = 0
                y_pos = 0
                r_size = 1
                c_size = 1
                self.MdLeftLayout.addWidget(self.__feature_groups[key],x_pos,y_pos, r_size, c_size, Qt.AlignTop)
            if key == "d_bookings":
                x_pos = 0
                y_pos = 1
                r_size = 1
                c_size = 1
                self.MdLeftLayout.addWidget(self.__feature_groups[key],x_pos,y_pos, r_size, c_size, Qt.AlignBottom)
            if key == "d_order_items":
                x_pos = 0
                y_pos = 2
                r_size = 1
                c_size = 1
                self.MdLeftLayout.addWidget(self.__feature_groups[key],x_pos,y_pos, r_size, c_size, Qt.AlignTop)
            if key == "d_orders":
                x_pos = 0
                y_pos = 1
                r_size = 1
                c_size = 1
                self.MdLeftLayout.addWidget(self.__feature_groups[key],x_pos,y_pos, r_size, c_size, Qt.AlignTop)
            if key == "d_order_add_on_purchases":
                x_pos = 0
                y_pos = 2
                r_size = 1
                c_size = 1
                self.MdLeftLayout.addWidget(self.__feature_groups[key],x_pos,y_pos, r_size, c_size, Qt.AlignBottom)
            if key == "d_order_revenue_items":
                x_pos = 2
                y_pos = 0
                r_size = 1
                c_size = 1
                self.MdLeftLayout.addWidget(self.__feature_groups[key],x_pos,y_pos, r_size, c_size, Qt.AlignTop)

        self.__feature_group_layout["Derived_data"] = Qt5.QVBoxLayout()
        self.__feature_groups["Derived_data"] = Qt5.QGroupBox("Derived_data")
        self.__feature_groups["Derived_data"].setLayout( self.__feature_group_layout["Derived_data"] )
        self.__checkbox_group["Derived_data"] = []
        self.__checkbox_group_names["Derived_data"] = []
        self.__checkbox_group_keys["Derived_data"] = []
        for key in self.__attr_list["Derived_data"].keys():      # get derived data features
            check_box = CheckBox(key)
            check_box.stateChangedstr.connect(self.update_selected)  # connect stateChanged signal to slot checkbox change batch processing
            self.__feature_group_layout["Derived_data"].addWidget(check_box)
            self.__checkbox_group["Derived_data"].append(check_box)
            self.__checkbox_group_names["Derived_data"].append(key)
            self.__checkbox_group_keys["Derived_data"].append(key)
        x_pos = 2
        y_pos = 1
        r_size = 1
        c_size = 1
        self.MdLeftLayout.addWidget(self.__feature_groups["Derived_data"],x_pos,y_pos, r_size, c_size, Qt.AlignTop)



        ### arrange buttons #################################
        button_group = Qt5.QGroupBox()
        button_layout = Qt5.QVBoxLayout()
        reset_button = Qt5.QPushButton("Reset")
        reset_button.clicked.connect(self.reset_selection)
        button_layout.addWidget(reset_button)
        confirm_button = Qt5.QPushButton("Confirm")
        confirm_button.clicked.connect(self.generate_sqlcode)
        button_layout.addWidget(confirm_button)
        x_pos = 2
        y_pos = 2
        r_size = 1
        c_size = 1
        button_group.setLayout(button_layout)
        self.MdLeftLayout.addWidget(button_group,x_pos,y_pos, r_size, c_size, Qt.AlignCenter)




        self.__group_feature.setLayout(self.MdLeftLayout)
        # set up layers of different layouts
        self.L_VLayout.addWidget(self.__group_filter)
        self.L_VLayout.addWidget(self.__group_feature)


        self.H_Layout.addLayout(self.L_VLayout)
        self.H_Layout.addLayout(self.R_VLayout)
        self.setLayout(self.H_Layout)
        # thread control
        self.thread_finished = False
    ##############################################################################################################################################################################
    @pyqtSlot(Qt5.QCheckBox)
    def checkbox_change(self, checkbox):
        # self.thread = QThread()
        # self.thread.start()
        self.worker = GenericWorker(self.update_selected, checkbox)
        self.worker.start.emit("hi")
        # self.thread.finished.connect(self.worker.deleteLater)
        # self.worker.moveToThread(self.thread)
        # self.worker.start.emit("hello")
        # print "hhh"
        # while self.thread_finished:
        #     print "thread ended/........"
        #     self.thread_finished = False
        #     self.thread.quit()
        #     self.thread.wait()


    def update_selected(self,checkbox):
        text = checkbox.text()
        if checkbox.isChecked():
            if text not in self.__selected_features:
                self.__selected_features.append(text)
                self.chain_event.emit(checkbox)

            # for array_ch, array_name in zip( self.__checkbox_group.values(), self.__checkbox_group_names.values() ):
            #     # print len(array_ch), len(array_name)
            #     if text in array_name:
            #         s = [i for i, u in enumerate(array_name) if u == text]
            #         # print s, len(array_ch), type(array_ch)
            #         for ob in s:
            #             # print array_ch[ob].text()
            #             if not array_ch[ob].isChecked():
            #                 array_ch[ob].setChecked(True)

        else:
            if text in self.__selected_features:
                self.__selected_features.remove(checkbox.text())
                self.chain_event.emit(checkbox)

            # for array_ch, array_name in zip( self.__checkbox_group.values(), self.__checkbox_group_names.values() ):
            #     # print len(array_ch), len(array_name)
            #     if text in array_name:
            #         s = [i for i, u in enumerate(array_name) if u == text]
            #         # print s, len(array_ch), type(array_ch)
            #         for ob in s:
            #             # print array_ch[ob].text()
            #             if array_ch[ob].isChecked():
            #                 array_ch[ob].setChecked(False)

        print self.__selected_features
    ##############################################################################################################################################################################
    @pyqtSlot(QCheckBox)
    def chain_handler(self, checkbox):
        text = checkbox.text()
        for array_ch, array_name in zip( self.__checkbox_group.values(), self.__checkbox_group_names.values() ):
            # print len(array_ch), len(array_name)
            if text in array_name:
                s = [i for i, u in enumerate(array_name) if u == text and array_ch[i] != checkbox ]
                print s, len(array_ch), type(array_ch)
                for ob in s:
                    array_ch[ob].toggleChanged.emit()

    ##############################################################################################################################################################################
    @pyqtSlot()
    def generate_sqlcode(self):
        if self.__selected_features:
            self.fetcher.remove_all_attr()
            self.generate_sel_features()
            self.__start_cal.date().toPyDate()
            self.fetcher.get_trip_env_custom()

        else:
            Qt5.QMessageBox.information(self, "Error: no feature selected.....", "No Features selected, please select features....")


    def generate_sel_features(self):
        for i, feature in enumerate( self.__selected_features ):
            feature_flag = False
            for key in self.__checkbox_group.keys():
                if feature_flag:
                    break
                for key_n, attr in zip( self.__checkbox_group_keys[key] , self.__checkbox_group_names[key] ):
                    # print key_n, attr
                    if feature == attr:
                        feature_flag = True
                        # type is Normal append
                        if key != "Derived_data" and attr not in self.__checkbox_group_names["Derived_data"]:
                            self.fetcher.select_attr_append(func_in=self.__attr_list["ibemining"][key]["tb_alias"]+"."+key_n, type="Normal")
                        else:
                            self.fetcher.select_attr_append(func_in=feature, type="Derived")
                        break

        print self.fetcher.get_feature_wrapper()

    ##############################################################################################################################################################################

    @pyqtSlot()
    def reset_selection(self):
        del self.__selected_features[:]
        for key, array_n in self.__checkbox_group.iteritems():
            for ch in array_n:
                ch.setChecked(False)









if __name__ == '__main__':
    libpaths = QApplication.libraryPaths()
    if sys.platform == 'darwin':
        libpaths.append("/Users/gausslee/Documents/Qt5.5/5.5/clang_64/plugins")
        QApplication.setLibraryPaths(libpaths)
    elif sys.platform == 'win32':
        libpaths.append("C:/Users/gausslee/Anaconda/envs/py34/Lib/site-packages/PyQt5/plugins")
        QApplication.setLibraryPaths(libpaths)


    app = QApplication(sys.argv)
    TripGui = TripEnvGui()
    sys.exit(app.exec_())