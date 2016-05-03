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
import operator

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
        self.actual_bins = 0
        self.b = np.array([])
        self.freq = []
        self.fit_flag = False
        self.map_tmp = {}
        self.bin_id_record={}

    def occurence_count(self, data=pd.Series()):
        """
        calculate the frequency for each unique id.
        :return: the occurrence for each unique id
        """
        occ = dict({})
        for val in data.values:
            occ[val] = 0
        for val in data.values:
            occ[val] += 1
        return occ

    def ordering_id_map(self, data=pd.Series()):
        """
        Map the original random ID to the ordering ID based on frequency
        :return: mapped data marked by ordering numbers
        """
        if not self.fit_flag:
            tmp_occurrence = self.occurence_count(data)
            tmp_sorted_bin = sorted(tmp_occurrence.items(), key=operator.itemgetter(1), reverse=True)
            tmp_sorted_bin = np.asarray( tmp_sorted_bin )
            for o,origid in enumerate(tmp_sorted_bin[:,0]):
                self.map_tmp[origid] = o
            print "finished mapping construction......"
        length = len(self.map_tmp)
        i= 0
        for key,u in zip(self.map_tmp.keys(), self.map_tmp.values()):
            i+= 1.0
            if i%100 == 0:
                print "finished %.5f percentage" %( float(i/length) *100 )

            row_index = data == key
            data.loc[row_index] = u

        print type(data), type(self.map_tmp)
        diff = set( data.values ) - set(self.map_tmp.values() )
        print diff
        if not diff:
            pass
        else:
            print "warning: there are new alien data sent in, please fit with the new data..."
            for p in list(diff):
                row_index = data==p
                data.loc[row_index] = np.nan
        return data

    def log_n(self, data, n):
        """
        log transformation of the input data
        :param data: input data
        :param n: n times log taken
        :return: transformed data
        """
        data = np.log(data)
        # print "%.2f th data result is %s" %(n, str(data))
        if n <= 1.00:
            # print "returned... %s" %str(data)
            return data
        else:
            return self.log_n(data + 1, n-1)

    def bin_id_map(self, p, transformed_id):
        """
        Map the ordering ID to bin ID
        :param p: an array which contains value scope for each bin
        :param transformed_id: transformed ordering ID
        :return:  data marked by bin ID
        """
        assert  isinstance(transformed_id, pd.Series), "check out your input type..."
        if not self.fit_flag:
            a = 0
            b = 0
            id_bin = 0
            for i in range(len(p)-1):
                a = b
                b = p[i+1]
                jd = transformed_id.between(a,b)
                if (jd==True).any():
                    id_bin += 1
                    transformed_id[jd] = id_bin
                    self.bin_id_record[id_bin] = [a,b]
            self.actual_bins = id_bin
        else:
            for key, u in zip(self.bin_id_record.keys(),self.bin_id_record.values()):
                transformed_id[transformed_id.between(u[0],u[1])] = key
            if transformed_id.hasnans:
                print "Nan value found and transfromed to %d" %(self.actual_bins+1)
                transformed_id = transformed_id.fillna(self.actual_bins+1)
        return transformed_id


    def fit(self, data=pd.Series(),show_diag=True):
        """
        Automatic binning process, the result is put in the self.data
        :param show_diag: True for showing the binning histogram
        :return:
        """
        data = self.ordering_id_map(data)
        # tmp_occurrence = self.occurence_count(data)
        # tmp_sorted_bin = sorted(tmp_occurrence.items(), key=operator.itemgetter(1), reverse=True)
        # tmp_sorted_bin = np.asarray( tmp_sorted_bin )
        # use matplotlib even-spaced binning
        trans_id = self.log_n(data + 1, 2)
        self.freq,self.b,p = plt.hist(trans_id , bins=self.max_bins)
        data = self.bin_id_map(self.b, trans_id)
        self.fit_flag = True
        if show_diag:
            plt.show()

    def transform(self, data = pd.Series()):
        """
        :param data: target data
        :return: transformed data with previous fit
        """
        if not self.fit_flag:
            raise Exception("You have not fitted your model yet...")
        data = self.ordering_id_map(data)
        trans_id = self.log_n(data+1, 2)

        tran_data = self.bin_id_map(self.b, trans_id)
        return tran_data







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
    binning =  BinEncoder(max_bins = 20)
    print "start to bin...."
    binning.fit(training_data["SiteID"],  False)
    data = binning.transform(training_data["SiteID"])
    print data.to_csv("data.csv")
    alien_data = pd.Series([-9999, 39, -19])
    alien_data = binning.transform(alien_data)
    print alien_data
