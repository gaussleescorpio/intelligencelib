### import all the libraries

import pandas as pd
import scipy
import numpy as np
from scipy import stats
from matplotlib import pylab as plt
import category_encoders as ce




def occrence_count_plot(X_in=[]):
    id_dict={}
    for e in X_in:
        if e in id_dict.keys():
            id_dict[e] += 1
        else:
            id_dict[e] = 1

    print id_dict

    import operator

    id_dict = sorted(id_dict.items(), key=operator.itemgetter(1), reverse=True)
    print id_dict
    id_dict = np.asarray(id_dict)
    x = id_dict[:,0]
    print stats.kstest(id_dict[:,1], 'norm')
    plt.bar( x, id_dict[:,1])
    plt.show()




### import the data from a csv file #####
data = pd.read_csv("data/newsql_data20132014n.csv")
raw_data = list(data)
print "data loaded..................."

# print data.columns.values
# print data.values.shape

### target and training data definition
target = data["Tb1"] - data["MarkupSek"]
target = np.asarray(target)
selection_cols = [ 'OrderDay','SiteID', 'SiteCountryID',
 'FirstAirSystem',
 'TravelDays', 'DaysAhead' , 'IsInterCont', 'JouneyType',
 'NumBounds', 'NumSegs', 'Bound2StartSeg1Bs', 'NumofAd',
 'NumofCh', 'Numofinf', 'ServDays', 'Net', 'TaxExVat', 'Vat',
 'MarkupSeg', 'BookType','OriginAirportID', 'OriginCityID','OriginCountryID', 'DestCityID', 'DestCoutID','MarketCostSek']
training_data = data[selection_cols]

# occrence_count_plot(data["OriginAirportID"])

#############################################################################
#############################################################################
# this is the coding rule for testing
###########################

# one of n coding
import scipy
from scipy import stats
from sklearn import preprocessing

norminal = ['SiteID','NumofAd','SiteCountryID','NumofCh',
            'Numofinf','MarkupSeg','JouneyType','BookType','FirstAirSystem',
            'NumBounds','NumSegs','Bound2StartSeg1Bs','DaysAhead']
t = training_data[norminal]
enc = preprocessing.OneHotEncoder()
enc.fit(t)
print enc.n_values_
print enc.active_features_
trans_t = enc.transform(t)
trans_t = trans_t.toarray()
print trans_t.shape
print type(trans_t)
print trans_t[1,:]


# binary coding


# enc = ce.BinaryEncoder(t)
# enc.fit(t)
# trans_t = enc.transform(t)
# print type(trans_t)
# print "transform finished.."

#############################################################################
# coding rule finished
#############################################################################
#############################################################################

############################################################################
# combining coded new data with original data
###########################################################################

trans_num = 1000000
start_num = 750000
num = 900000
# take out the leftovers and assemble with 1 of n coding

#left_over = np.setxor1d(selection_cols, norminal)
left_over = ['IsInterCont','Net','TaxExVat', 'Vat', 'MarketCostSek']
print left_over
left_data = training_data[left_over]
print left_data.shape
left_data = left_data.as_matrix()
print type(left_data)
tmp = np.zeros((trans_t.shape[0], trans_t.shape[1] + left_data.shape[1]))
new_list = [trans_t[0:trans_num,:], left_data[0:trans_num,:]]
new_data = np.hstack(new_list)
print new_data.shape
# np.savetxt("new_data.csv", new_data, delimiter=",")
print "save successful..."
print target.shape


# preparing the data split for training and testing
test_num = new_data.shape[0] - num
train_data = new_data[start_num:num,:]
train_target = target[start_num:num]
testing_data = new_data[num:trans_num-1,:]
testing_target = target[num:trans_num-1]
print testing_data.shape
print testing_target.shape
###########################################################################
#combining finished.....
###########################################################################
###########################################################################

### start with normal linear regression
from sklearn import  linear_model
regr = linear_model.LinearRegression(fit_intercept=True,n_jobs=-1)
regr.fit(train_data, train_target)
plt.ylim(-1000,1000)
plt.plot(regr.predict(testing_data)-testing_target, '*', color='blue',linewidth=3)
print regr.predict(testing_data), testing_target
# toggle this if you wanna say error statistics in a plot
# plt.show(block=True)


### Lasso Regression
X = train_data
y = list(train_target)
lasso_regr = linear_model.Lasso(alpha=0.1, max_iter=10e5)
lasso_regr.fit(X,y)
lasso_lar = linear_model.LassoLars(alpha=0.1, max_iter=10e7)
lasso_lar.fit(X,y)
print lasso_regr.predict(testing_data), testing_target
print lasso_lar.predict(testing_data), testing_target
plt.ylim(-1000,1000)
# toggle this if you wanna say error statistics in a plot
# plt.plot(lasso_regr.predict(testing_data)-testing_target, '*', color='blue',linewidth=3)
# plt.plot(lasso_lar.predict(testing_data)-testing_target, '*', color='blue',linewidth=3)
print target

###########################################################################
# statistics shown
###########################################################################
###########################################################################

# statistics for normal linear regression
i = 0
corr = 0
print "starting......"
for u in testing_target:
    # print u, testing_target[i]
    if u > 0:
        corr += 1.0
    else:
        pass
        #print u, testing_target[i]
    i += 1.0

DummyP = corr / i
print DummyP



i = 0.0
corr = 0.0
corrp = 0.0
corrn = 0.0
print "starting......"
for u in regr.predict(testing_data):
    # print u, testing_target[i]
    if (u > 0 and testing_target[i]>0) or (u < 0 and testing_target[i]<0):
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            corrp +=1.0
        if (u < 0 and testing_target[i]<0):
            corrn += 1.0
    else:
        pass
        #print u, testing_target[i]
    i += 1.0

regr_a = corr / i
print "Normal regression correction rate:" + str(regr_a)
print " "*12 + "predicted_pos" + " "*4 + "predicted_neg"
print "actual_pos" + " "*2 + str(corrp/i) + " "*3 + str(DummyP-corrp/i)
print "actual_neg" + " "*2 + str(1-DummyP - corrn/i) + " "*3 + str(corrn/i)
print "this is the regr..........%s" %regr
print 'Variance score: %.2f' % regr.score(testing_data, testing_target)
print "Mean Square Error: %.4f" %  np.sqrt ( sum( ( regr.predict(testing_data)- testing_target )**2 ) / len(testing_target) )


##############
# statistics for lasso linear regression
i = 0.0
corr = 0.0
corrp = 0.0
corrn = 0.0
print "starting......"
for u in lasso_regr.predict(testing_data):##regr_raw.predict(data.values[100000:-1,:]):##
    # print u, testing_target[i]
    if (u > 0 and testing_target[i]>0) or (u < 0 and testing_target[i]<0):
        #print u, testing_target[i]
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            corrp +=1.0
        if (u < 0 and testing_target[i]<0):
            corrn += 1.0
    else:
        pass
        #print u, testing_target[i]
    i += 1.0

lasso_a = corr / i
print "lasso regression correction rate:" + str(lasso_a)
print " "*12 + "predicted_pos" + " "*4 + "predicted_neg"
print "actual_pos" + " "*2 + str(corrp/i) + " "*3 + str(DummyP-corrp/i)
print "actual_neg" + " "*2 + str(1-DummyP - corrn/i) + " "*3 + str(corrn/i)
print('Variance score: %.2f' % lasso_regr.score(testing_data, testing_target))
print "Mean Square Error: %.4f" %  np.sqrt ( sum( ( lasso_regr.predict(testing_data)- testing_target )**2 ) / len(testing_target) )
###########

i = 0.0
corr = 0.0
corrp = 0.0
corrn = 0.0
print "starting......"
for u in lasso_lar.predict(testing_data):
    # print u, testing_target[i]
    if (u > 0 and testing_target[i]>0) or (u < 0 and testing_target[i]<0):
        #print u, testing_target[i]
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            corrp +=1.0
        if (u < 0 and testing_target[i]<0):
            corrn += 1.0
    else:
        pass
        #print u, testing_target[i]
    i += 1.0

lasso_lar_a = corr / i
print "lasso_lar regression correction rate:" + str(lasso_lar_a)
print " "*12 + "predicted_pos" + " "*4 + "predicted_neg"
print "actual_pos" + " "*2 + str(corrp/i) + " "*3 + str(DummyP-corrp/i)
print "actual_neg" + " "*2 + str(1-DummyP - corrn/i) + " "*3 + str(corrn/i)
print('Variance score: %.2f' % lasso_lar.score(testing_data, testing_target))
print "Mean Square Error: %.4f" %  np.sqrt ( sum( ( lasso_lar.predict(testing_data)- testing_target )**2 ) / len(testing_target) )



