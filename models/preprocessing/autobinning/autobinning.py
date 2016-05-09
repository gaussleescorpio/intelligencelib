"""
automatic binning algorithm with evenly spaced bins. The user can assign the number of bins and the target data with random ID numbers.
The purpose of this module is to reduce the high dimensionalities caused by fully-fledged dummy coding.

Input: the data as pandas dataframe and num_bins
Output: the binned data marked by bin id

The binning criterion is based on the frequency/occurrence of each unique ID. The output can directly replace original data and be used with
dummy coding to generate low dimensional data.
"""

import pandas as pd
import scipy
import numpy as np
from scipy import stats
from matplotlib import pylab as plt
import category_encoders as ce
from datetime import datetime

# silience the chained-assignment warning
pd.set_option('chained_assignment',None)

class BinEncoder():
    def __init__(self, max_bins=5):
        """
        :param data: input data that must be a pandas dataframe
        :param num_bins: intended num of bins
        :return:
        """
        self.max_bins = max_bins
        self.actual_bin_num = 0
        self.b = np.array([])
        self.freq = []
        self.map_rec = pd.Series()
        self.bin_id_record={}

    def count_occurrences(self, data=pd.Series()):
        """
        calculate the frequency for each unique id.
        :return: the occurrence for each unique id
        """
        return data.value_counts()

    def order_map(self, data=pd.Series()):
        """
        Map the original random ID to the ordering ID based on frequency
        :return: mapped data marked by ordering numbers
        """
        posns_in_order = data.copy()
        occurrence_sorted = self.count_occurrences(posns_in_order)
        self.map_rec = occurrence_sorted.copy()
        for o,origid in enumerate(occurrence_sorted.keys()):
            self.map_rec[origid] = o + 1
        print "finished mapping origid-o construction......"
        length = len(self.map_rec)
        i= 0

        for key,u in zip( self.map_rec.keys(), self.map_rec.values ):
            i+= 1.0
            if i%100 == 0:
                print "finished %.5f percentage" %( float(i/length) *100 )

            row_index = data == key
            posns_in_order.loc[row_index] = u

        return posns_in_order

    def log_n(self, X, n):
        """
        log transformation of the input data
        :param data: input data
        :param n: n times log taken
        :return: transformed data
        """
        transformed_X = np.log(X)
        # print "%.2f th data result is %s" %(n, str(data))
        if n <= 1.00:
            # print "returned... %s" %str(data)
            return transformed_X
        else:
            return self.log_n(transformed_X+1, n-1)

    def bin_map(self, p):
        """
        Map the ordering ID to bin ID
        :param p: an array which contains value scope for each bin
        :param transformed_id: transformed ordering ID
        :return:
        """
        a = 0
        b = 0
        id_bin = 0
        map_mark = self.map_rec.copy()
        for i in range(len(p)-1):
            a = b
            b = p[i+1]
            jd = map_mark.between(a,b)
            if (jd==True).any():
                id_bin += 1
                self.map_rec[jd] = id_bin
                self.bin_id_record[id_bin] = [a,b]
        self.actual_bin_num = id_bin


    def fit(self, X=pd.Series(),show_diag=True):
        """
        Automatic binning process, the result is put in the self.data
        :param show_diag: True for showing the binning histogram
        :return:
        """

        mapped_data = self.order_map(X)
        mapped_data = self.log_n(mapped_data, 2)
        self.map_rec = self.log_n(self.map_rec, 2)

        self.freq,self.b,p = plt.hist(mapped_data , bins=self.max_bins)
        self.bin_map(self.b)
        # print self.actual_bin_num
        print "fitting process finished...."
        if show_diag:
            plt.show()


    def transform(self, X = pd.Series()):
        """
        :param X: target X
        :return: transformed X with previous fit
        """
        if self.actual_bin_num == 0:
            raise Exception("You have not fitted your model yet...")
        # directly map the id to bin_id with trained map
        copy_X = X.copy()
        # t1 = datetime.now()
        for key in self.map_rec.keys():
            index_rows = copy_X == key
            X[index_rows] = self.map_rec[key]
        # t2 = datetime.now()
        # print t2-t1

        diff = set(X.values) - set(self.map_rec.values)
        if not diff:
            pass
        else:
            print "warning: there are new alien data sent in, please fit with the new data:%s" %str(diff)
            for p in list(diff):
                row_index = copy_X==p
                X[row_index] = self.actual_bin_num + 1

        return X







if __name__ == "__main__":
    # path where you put the testing data
    path = "/Users/gausslee/Documents/programming/etravelipython/ibemingdata/newsql_data20132014n.csv"
    data = pd.read_csv(path)
    raw_data = list(data)
    print "data loaded..................."
    selection_cols = [ 'OrderDay','SiteID', 'SiteCountryID',
     'FirstAirSystem',
     'TravelDays', 'DaysAhead' , 'IsInterCont', 'JouneyType',
     'NumBounds', 'NumSegs', 'Bound2StartSeg1Bs', 'NumofAd',
     'NumofCh', 'Numofinf', 'ServDays', 'Net', 'TaxExVat', 'Vat',
     'MarkupSeg', 'BookType','OriginAirportID', 'OriginCityID','OriginCountryID', 'DestCityID', 'DestCoutID','MarketCostSek']
    training_data = data[selection_cols]
    binning =  BinEncoder(max_bins = 40)
    print "start to bin...."
    binning.fit(training_data["SiteCountryID"],  True)
    data = binning.transform(training_data["SiteCountryID"])
    print data.to_csv("data.csv")
    alien_data = pd.Series([-9999, 39, -19])
    alien_data = binning.transform(alien_data)
    print alien_data
