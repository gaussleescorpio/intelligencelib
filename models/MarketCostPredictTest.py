import pandas as pd
import scipy
import numpy as np
from scipy import stats
from matplotlib import pylab as plt
import mxnet as mx
import datetime
import category_encoders as ce

def get_data_exclusive(data= pd.DataFrame({}),name="", ex_name=""):
    return data[data[name]!=ex_name]

### import the data from a csv file #####
a = datetime.datetime.now()
data = pd.read_excel("data/MiscStat.xlsx-20160201.xls")

data = get_data_exclusive(data, "PartnerDomain", "google")
# data = get_data_exclusive(data, "PartnerDomain", "momondo")
data = data.replace(np.nan,' ', regex=True)
data = get_data_exclusive(data,"PartnerDomain",' ')


data1 = pd.read_excel("data/MiscStat.xlsx-20160202.xls")
data1 = get_data_exclusive(data1, "PartnerDomain", "google")
# data = get_data_exclusive(data, "PartnerDomain", "momondo")
data1 = data1.replace(np.nan,' ', regex=True)
data1 = get_data_exclusive(data1,"PartnerDomain",' ')


data = data.append(data1)
data1 = pd.read_excel("data/MiscStat.xlsx-20160203.xls")
data1 = get_data_exclusive(data1, "PartnerDomain", "google")
# data = get_data_exclusive(data, "PartnerDomain", "momondo")
data1 = data1.replace(np.nan,' ', regex=True)
data1 = get_data_exclusive(data1,"PartnerDomain",' ')



# data1 = pd.read_excel("data/MiscStat.xlsx-20160204.xls")
# data = data.append(data1)
#
# data1 = pd.read_excel("data/MiscStat.xlsx-20160205.xls")
# data = data.append(data1)
# data1 = pd.read_excel("data/MiscStat.xlsx-20160206.xls")
# data = data.append(data1)
# data1 = pd.read_excel("data/MiscStat.xlsx-20160207.xls")
# data = data.append(data1)
# data1 = pd.read_excel("data/MiscStat.xlsx-20160208.xls")
# data = data.append(data1)
# data1 = pd.read_excel("data/MiscStat.xlsx-20160209.xls")


# df = data[data.PartnerDomain == "kayak"]
# df = df[df.Site == "gotogate_fr"]
# print df.OrderDateTime
# plt.plot(range(len(df)), df.MrkCost)
# plt.show()



train_size = len(data)
test_size = len(data1)
data = data.append(data1)
raw_data = list(data)
print "data loaded..................."
print datetime.datetime.now() - a
print data.keys()






### target and training data definition
target = data['MrkCost'].as_matrix()
selection_cols = [ 'Site',
 'PartnerDomain']
training_data = data[selection_cols]

#############################################################################
#############################################################################
# this is the coding rule for testing
###########################

# one of n coding
import scipy
from scipy import stats
from sklearn import preprocessing

# label the string as nominal
norminal = ['Site',
 'PartnerDomain']
lable_make = preprocessing.LabelEncoder()
training_data = training_data.as_matrix()

for i in range(len(norminal)):
    training_data[:,i] = lable_make.fit_transform(training_data[:,i])


t = training_data

# coding

#### one hot coding ################
enc = preprocessing.OneHotEncoder()
enc.fit(t)
print enc.n_values_
print enc.active_features_
trans_t = enc.transform(t)
trans_t = trans_t.toarray()


# binary coding


# enc = ce.BinaryEncoder(t)
# enc.fit(t)
# trans_t = enc.transform(t)
# print type(trans_t)
# print "transform finished.."

#### add left over codes contenance here if needed#####

#######

new_data = trans_t
trans_num = test_size  + train_size
start_num = 0
num = train_size

print start_num, num, trans_num
# preparing the data split for training and testing
test_num = new_data.shape[0] - num
train_data = new_data[start_num:num,:]
train_target = target[start_num:num]
testing_data = new_data[num:trans_num-1,:]
testing_target = target[num:trans_num-1]
print testing_data.shape
print testing_target.shape




#############################################################################
#############################################################################
# liear regression
###########################

### start with normal linear regression
from sklearn import  linear_model
regr = linear_model.LinearRegression(fit_intercept=True,n_jobs=-1)
regr.fit(train_data, train_target)
plt.ylim(-100,100)


plt.plot(regr.predict(testing_data)-testing_target, '*', color='blue',linewidth=3)
prediction_data = regr.predict(testing_data)
print type(prediction_data)
#print regr.predict(testing_data), testing_target
#plt.show()


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
    if (u > 0 and testing_target[i]>0) or (u <= 0 and testing_target[i]<=0):
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            corrp +=1.0
        if (u <= 0 and testing_target[i]<=0):
            #print "zero boundary prediction: %.2f, %.2f" % (u, testing_target[i])
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
print prediction_data - testing_target
print sum( ( prediction_data - testing_target )**2 )
print "Mean Square Error: %.4f" %  np.sqrt ( sum( ( prediction_data - testing_target )**2 ) / len(testing_target) )



### Lasso Regression: smaller alpha, better prediction accuracy
X = train_data
y = list(train_target)
lasso_regr = linear_model.Lasso(alpha=0.001, max_iter=10e5)
lasso_regr.fit(X,y)
lasso_lar = linear_model.LassoLars(alpha=0.001, max_iter=10e7)
lasso_lar.fit(X,y)
print lasso_regr.predict(testing_data), testing_target
print lasso_lar.predict(testing_data), testing_target
plt.ylim(-10,1000)
# toggle this if you wanna see error statistics in a plot
# plt.plot(lasso_regr.predict(testing_data),testing_target, '*', color='blue',linewidth=3)
# plt.plot(lasso_lar.predict(testing_data)-testing_target, '*', color='blue',linewidth=3)
# plt.show()


##############
# statistics for lasso linear regression
i = 0.0
corr = 0.0
corrp = 0.0
corrn = 0.0
print "starting......"
for u in lasso_regr.predict(testing_data):##regr_raw.predict(data.values[100000:-1,:]):##
    # print u, testing_target[i]
    if (u > 0 and testing_target[i]>0) or (u <=0 and testing_target[i]<=0):
        #print u, testing_target[i]
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            # print "positive prediction: %.2f, %.2f" % (u, testing_target[i])
            corrp +=1.0
        if (u <=0 and testing_target[i]<=0):
            # print "zero boundary prediction: %.2f, %.2f" % (u, testing_target[i])
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
    if (u > 0 and testing_target[i]>0) or (u <=0 and testing_target[i]<=0):
        #print u, testing_target[i]
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            corrp +=1.0
        if (u <=0 and testing_target[i]<=0):
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



############## support vector regression nonlinear#############################
from sklearn.svm import SVR
from sklearn import grid_search
svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1, epsilon=0.1)
svr_rbf.fit(X,y)

# svr = SVR()
# param = {'kernel':('poly', 'rbf'), 'C':[1, 10], 'gamma':[0.01,0.1,1,10], 'epsilon':[0.01,0.1,1]}
# svr_rbf = grid_search.GridSearchCV(svr, param)
# svr_rbf.fit(X,y)


i = 0.0
corr = 0.0
corrp = 0.0
corrn = 0.0
print "starting......"
for u in svr_rbf.predict(testing_data):
    if abs(u - testing_target[i]) > 20:
        print "predicted value is: %f" %u
        print "%s th target is:" %str(i)
        print data.values[num+i]
    if (u > 0 and testing_target[i]>0) or (u <=0 and testing_target[i]<=0):
        #print u, testing_target[i]
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            # print "positive prediction: %.2f, %.2f" % (u, testing_target[i])
            corrp +=1.0
        if (u <=0 and testing_target[i]<=0):
            # print "zero prediction %.2f, %.2f" % (u, testing_target[i])
            corrn += 1.0
    else:
        pass
        #print u, testing_target[i]
    i += 1.0

lasso_lar_a = corr / i
print "SVM regression correction rate:" + str(lasso_lar_a)
print " "*12 + "predicted_pos" + " "*4 + "predicted_neg"
print "actual_pos" + " "*2 + str(corrp/i) + " "*3 + str(DummyP-corrp/i)
print "actual_neg" + " "*2 + str(1-DummyP - corrn/i) + " "*3 + str(corrn/i)
print('Variance score: %.2f' % svr_rbf.score(testing_data, testing_target))
print "Mean Square Error: %.4f" %  np.sqrt ( sum( ( svr_rbf.predict(testing_data)- testing_target )**2 ) / len(testing_target) )
plt.plot(svr_rbf.predict(testing_data)-testing_target, '*', color='blue',linewidth=3)
plt.show()


####### testing model fro RandomForestRegression #############################
from sklearn.ensemble import RandomForestRegressor

RF = RandomForestRegressor(n_estimators=22, verbose=1, n_jobs=-1)
RF.fit(X,y)


i = 0.0
corr = 0.0
corrp = 0.0
corrn = 0.0
for u in RF.predict(testing_data):
#     if abs(u - testing_target[i]) > 20:
#         print "predicted value is: %f" %u
#         print "%s th target is:" %str(i)
#         print data.values[num+i]
    if (u > 0 and testing_target[i]>0) or (u <=0 and testing_target[i]<=0):
        #print u, testing_target[i]
        corr += 1.0
        if (u > 0 and testing_target[i]>0):
            # print "positive prediction: %.2f, %.2f" % (u, testing_target[i])
            corrp +=1.0
        if (u <=0 and testing_target[i]<=0):
            # print "zero prediction %.2f, %.2f" % (u, testing_target[i])
            corrn += 1.0
    else:
        pass
        #print u, testing_target[i]
    i += 1.0

lasso_lar_a = corr / i
print "RF regression correction rate:" + str(lasso_lar_a)
print " "*12 + "predicted_pos" + " "*4 + "predicted_neg"
print "actual_pos" + " "*2 + str(corrp/i) + " "*3 + str(DummyP-corrp/i)
print "actual_neg" + " "*2 + str(1-DummyP - corrn/i) + " "*3 + str(corrn/i)
print('Variance score: %.2f' % RF.score(testing_data, testing_target))
print "Root Mean Square Error: %.4f" %  np.sqrt ( sum( ( RF.predict(testing_data)- testing_target )**2 ) / len(testing_target) )
plt.plot(RF.predict(testing_data)-testing_target, '*', color='blue',linewidth=3)
plt.show()

