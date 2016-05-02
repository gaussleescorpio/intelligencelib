from ServerConnectionLocal import IbeFetcher
import pandas as pd
import sys
import time
import json
import operator



class AdvanceDataFetcher(IbeFetcher):
    def __init__(self, host_name="bisam-client01", user_name="gauss", pass_word="gauss"):
        IbeFetcher.__init__(self,hostname=host_name,username=user_name,password=pass_word)
        self.__attr_list = self.json_attr()
        self.__sel_features = []
        self.__feature_sql_wrapper = []
        self.__inner_join_wrapper = []
        self.__filter_wrapper=[]
        self.__aggre_wrapper=[]
        self.__table_track = {}


    def json_attr(self):
        with open("attributes.json",'r') as fb:
            ss = json.load(fb)
        return ss

    def get_attr_list(self):
        return self.__attr_list

    def get_features(self):
        return self.__sel_features

    def get_feature_wrapper(self):
        return self.__feature_sql_wrapper

    def get_table_tracker(self):
        return self.__table_track

    def get_inner_join_wrapper(self):
        return  self.__inner_join_wrapper

    def get_aggregation(self):
        return self.__aggre_wrapper

    def __select_attr_append(self, func_in = "da.Site_ID"):
        attr_exist = False
        table_name = None
        tb = func_in.split(".")[0]
        if tb in self.__attr_list["ibemining"]["alias"].keys():
            table_name = self.__attr_list["ibemining"]["alias"][ tb ]
            attr_exist = True
        # for tb in self.__attr_list["ibemining"].keys():
        #     if func_in.split(".")[1] in self.__attr_list["ibemining"][tb]:
        #         table_name = tb
        #         attr_exist = True
        #         break

        if attr_exist:
            if table_name in self.__table_track.keys():
                self.__table_track[table_name] += 1
            else:
                self.__table_track[table_name] = 1
            self.__sel_features.append(func_in)
            self.__feature_sql_wrapper.append(func_in + " as %s" \
                                       %( self.__attr_list["ibemining"][table_name][func_in.split(".")[1]] ) + "\n")
        else:
            raise AssertionError("wrong input feature name, please check your input name......")

    def remove_attr(self, func_in = "da.Site_ID", type = "Normal"):
        # remove eles from table tracker
        if type == "Normal":
            tb_alias = func_in.split(".")[0]
            # remove table name
            if self.__attr_list["ibemining"]["alias"][tb_alias] in self.__table_track.keys():
                self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alias] ] -= 1
                if self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alias] ] <= 0:
                    del self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alias] ]

            else:
                raise AssertionError("cannot find the table, please check the table alias name....")

        if type == "Derived":
            tb_alias = self.__attr_list["Derived_data"][func_in]["tables"]
            for tb_alia in tb_alias:
                if tb_alia in self.__attr_list["ibemining"]["alias"]:
                    if self.__attr_list["ibemining"]["alias"][tb_alia] in self.__table_track.keys():
                        self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alia] ] -= 1
                        if self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alia] ] <= 0:
                            del self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alia] ]

                if tb_alia in self.__attr_list["ibe"]["alias"]:
                    if self.__attr_list["ibe"]["alias"][tb_alia] in self.__table_track.keys():
                        self.__table_track[ self.__attr_list["ibe"]["alias"][tb_alia] ] -= 1
                        if self.__table_track[ self.__attr_list["ibe"]["alias"][tb_alia] ] <= 0:
                            del self.__table_track[ self.__attr_list["ibe"]["alias"][tb_alia] ]

        deleted = False
        # remove from feature array
        for i, name in enumerate(self.__sel_features):
            if name == func_in:
                deleted = True
                del self.__sel_features[i]
                del self.__feature_sql_wrapper[i]
                break

        if deleted:
            pass
        else:
            raise AssertionError("cannot find the feature, please check the feature name")

    def remove_all_attr(self):
        del self.__sel_features[:]
        del self.__feature_sql_wrapper[:]
        self.__table_track.clear()
        del self.__inner_join_wrapper[:]
        del self.__aggre_wrapper[:]



    def select_attr_append(self, func_in = "TravelDays", type="Normal"):
        if type == "Normal":
            self.__select_attr_append(func_in)
        if type == "Derived":
            if func_in in self.__attr_list["Derived_data"].keys():
                self.__sel_features.append(func_in)
                wrapper = self.__attr_list["functions"][self.__attr_list["Derived_data"][func_in]["op"]] % \
                          tuple(self.__attr_list["Derived_data"][func_in]["param"]) + " as %s" %func_in +"\n"
                self.__feature_sql_wrapper.append(wrapper)
                # track the table
                tb_alias = self.__attr_list["Derived_data"][func_in]["tables"]
                table_tracked = False
                for tb_alia in tb_alias:
                    if tb_alia in self.__attr_list["ibemining"]["alias"]:
                        table_tracked = True
                        if self.__attr_list["ibemining"]["alias"][tb_alia] in self.__table_track:
                            self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alia] ] += 1
                        else:
                            self.__table_track[ self.__attr_list["ibemining"]["alias"][tb_alia] ] = 1

                    if tb_alia in self.__attr_list["ibe"]["alias"]:
                        table_tracked = True
                        if self.__attr_list["ibe"]["alias"][tb_alia] in self.__table_track:
                            self.__table_track[ self.__attr_list["ibe"]["alias"][tb_alia] ] += 1
                        else:
                            self.__table_track[ self.__attr_list["ibe"]["alias"][tb_alia] ] = 1
                    if table_tracked == False:
                        raise  AssertionError("Please check the json file for correct table alias or names....")

            else:
                raise AssertionError("Error in your input feature name, cannot find it...." )

    def equal_attr(self, attr1, attr2):
        if attr1 == attr2:
            return True
        if "Order_Created" in [attr1, attr2] \
            and "Created" in [attr1, attr2]:
            return True
        if "curr_id_to" in [attr1,attr2] \
            and "Currency_ID" in [attr1,attr2]:
            return True
        if "id" in [attr1,attr2] \
            and "Destination_Country_ID" in [attr1, attr2]:
            return True
        if "id" in [attr1,attr2] \
            and "Origin_Country_ID" in [attr1,attr2]:
            return True

        return False

    # def find_common(self, array1, array2):


    def __join_tables(self):
        self.__inner_join_wrapper.append("from ibemining.d_air_bookings as da \n")

        for tb_name in self.__table_track.keys():
            if tb_name == "d_air_booking":
                continue
            else:
                # judge if the table name is in db ibemining
                if tb_name in self.__attr_list["ibemining"].keys():
                    # get the alias name of the table, if there are two alias use a loop
                    for alia_name in self.__attr_list["ibemining"][tb_name]["tb_alias"].split(","):
                        wrapper = "inner outer join " + "ibemining.%s"%(tb_name) + " as %s"%(alia_name) \
                                 + " on "
                        on_symbol = "%s and "*len( self.__attr_list["join_info"]["ibemining"][alia_name] )
                        on_symbol = on_symbol[0:-5]
                        cond = []
                        for j_tb_name in self.__attr_list["join_info"]["ibemining"][alia_name].keys():
                            cond.append(alia_name+"."+j_tb_name+"="+self.__attr_list["join_info"]["ibemining"][alia_name][j_tb_name])

                        # print "......."*10
                        # print cond
                        # print on_symbol
                        # print wrapper
                        # print "......."*10
                        wrapper += on_symbol%tuple(cond)+"\n"
                # judge if the table name is in db ibe
                if tb_name in self.__attr_list["ibe"].keys():
                    # get the alias name of the table, if there are two alias use a loop
                    for alia_name in self.__attr_list["ibe"][tb_name]["tb_alias"].split(","):
                        wrapper = "inner outer join " + "ibe.%s"%(tb_name) + " as %s"%(alia_name) \
                                 + " on "
                        on_symbol = "%s and "*len( self.__attr_list["join_info"]["ibe"][alia_name] )
                        on_symbol = on_symbol[0:-5]
                        cond = []
                        for j_tb_name in self.__attr_list["join_info"]["ibe"][alia_name].keys():
                            cond.append(alia_name+"."+j_tb_name+"="+self.__attr_list["join_info"]["ibe"][alia_name][j_tb_name])
                        wrapper += on_symbol%tuple(cond)+"\n"

                self.__inner_join_wrapper.append(wrapper)

    def __join_tables_custom(self):
        import numpy as np
        if len(self.__table_track) <= 1:
            for tb_name in self.__table_track.keys():
                if tb_name in self.__attr_list["ibemining"].keys():
                    wrapper = "from ibemining.%s as %s \n"%( tb_name,  self.__attr_list["ibemining"][tb_name]["tb_alias"] )
                if tb_name in self.__attr_list["ibe"].keys():
                    wrapper = "from ibemining.%s as %s \n"%( tb_name,  self.__attr_list["ibe"][tb_name]["tb_alias"] )
            self.__inner_join_wrapper.append(wrapper)
            return

        if len(self.__table_track) >= 2:
            sel_feature = {}
            main_tb_name = max(self.__table_track.iteritems(), key=operator.itemgetter(1))[0]
            # turn the whole feature list into a dict you can search with keys
            for sel in self.__sel_features:
                tb_alia = sel.split(".")[0]
                feature = sel.split(".")[1]
                if tb_alia in self.__attr_list["ibemining"]["alias"].keys():
                    tb = self.__attr_list["ibemining"]["alias"][tb_alia]
                    if not sel_feature.has_key(tb):
                        sel_feature[tb] = np.asarray( [feature, self.__attr_list["ibemining"][tb][feature] ] )
                    else:
                        sel_feature[tb] = np.concatenate(( sel_feature[tb]\
                                                               ,np.asarray( [feature, self.__attr_list["ibemining"][tb][feature] ] ) ), axis=0)

                if tb_alia in self.__attr_list["ibe"]["alias"].keys():
                    tb = self.__attr_list["ibe"]["alias"][tb_alia]
                    if not sel_feature.has_key(tb):
                        sel_feature[tb] = np.asarray( [feature, self.__attr_list["ibe"][tb][feature] ] )
                    else:
                        sel_feature[tb].append(np.asarray( [feature, self.__attr_list["ibe"][tb][feature] ] ))

            if main_tb_name in self.__attr_list["ibemining"].keys():
                wrapper = "from ibemining.%s as %s \n"%( main_tb_name,  self.__attr_list["ibemining"][main_tb_name]["tb_alias"] )
            if main_tb_name in self.__attr_list["ibe"].keys():
                wrapper = "from ibemining.%s as %s \n"%( main_tb_name,  self.__attr_list["ibe"][main_tb_name]["tb_alias"] )

            for tb_nm in self.__table_track.keys():
                base_tb = main_tb_name
                if tb_nm == main_tb_name:
                    continue
                set_a = sel_feature[main_tb_name][:,1]
                set_b = sel_feature[tb_nm][:,1]
                set_c = set(set_a) & set(set_b)
                tmp = sel_feature.pop(main_tb_name)
                if list(set_c):

                    # go to check with the other tables
                    for search_tb in sel_feature.keys():
                        set_a = sel_feature[search_tb]
                        set_c = set(set_a) & set(set_b)
                        if set_c:
                            base_tb = search_tb
                            break
                else:
                    sel_feature[main_tb_name] = tmp
                    break
                # add joins to wrapper


                sel_feature.pop(tb_nm)








    def __filter(self, name = "time"):
        if name in self.__attr_list["filter"].keys():
            fil = self.__attr_list["filter"][name]
            if (fil + "\n") in self.__filter_wrapper:
                return
            self.__filter_wrapper.append(fil + "\n")
        else:
            raise AssertionError("no %s option for filters.., please update your json file" %name)

    def __aggregation(self):
        common = []
        ibemining_marker = 0

        if len(self.__table_track) <= 1:
            return
        else:
            # check number of tables in ibemining
            ibemining_marker = len( [u for u in self.__table_track.keys() if u in self.__attr_list["ibemining"]] )
            print "ibemining_marker: " + str(ibemining_marker)
            for i in range( 0,len( self.__table_track.keys() ) - 1 ):

                if i == 0 or not common:
                    tb1 = self.__table_track.keys()[i]
                    if tb1 in self.__attr_list["ibemining"].keys():
                        tb1_attrs = self.__attr_list["ibemining"][tb1].values()

                    if tb1 in self.__attr_list["ibe"].keys():
                        if ibemining_marker < 2:
                            tb1_attrs = self.__attr_list["ibe"][tb1].values()
                        else:
                            continue

                    common = tb1_attrs
                print common
                tb2 = self.__table_track.keys()[i+1]
                if tb2 in self.__attr_list["ibemining"].keys():
                    tb2_attrs = self.__attr_list["ibemining"][tb2].values()
                if tb2 in self.__attr_list["ibe"].keys():
                    if ibemining_marker <2:
                        tb2_attrs = self.__attr_list["ibe"][tb2].values()
                    else:
                        continue

                # print tb2_attrs
                # find common elements of two arrays

                common = set(common) & set(tb2_attrs)
        print common
        if "OrderTime" in common:
            self.__aggre_wrapper.append("OrderTime")
            return
        if "Order_ID" in common:
            self.__aggre_wrapper.append("Order_ID")
            return
        self.__aggre_wrapper = list( common )

    def get_trip_env_custom(self, filter=["time"], filter_input=[tuple( ("2012-07-01","2012-07-02") )]):
        for u in filter:
            self.__filter(name=u)
        self.__aggregation()
        command = "select \n"
        for feature in self.__feature_sql_wrapper:
            command += feature

        self.__join_tables()
        for u in self.__inner_join_wrapper:
            command += u

        command += "where "
        for i, fil in enumerate(self.__filter_wrapper):
            print "....."*10
            print fil,filter_input
            command += fil %filter_input[i]

        if self.__aggre_wrapper:
            command += "group by \n"
            for agg in self.__aggre_wrapper:
                command += agg + "\n"

            command += "order by %s" %self.__aggre_wrapper[0]

        print command
        return command





    def get_trip_env_default(self, start_date="2012-07-01",end_date="2012-07-02"):
        commands = """ select
                da.Order_Created as OrderTime,
                date(da.Order_Created) as OrderDate,
                dayofweek(da.Order_Created) as OrderDay,
                da.Order_ID as OrderID,
                da.Site_ID as SiteID,
                da.Site_Country_ID as SiteCountryID,
                da.System_ID as FirstAirSystem,
                da.Provider_Booking_ID as PbookingID,
                da.Departure_Time_First_Segment as DepartureTimeFL,
                da.Arrival_Time_Last_Segment as ArrivalTimeLS,
                datediff(da.Arrival_Time_Last_Segment, da.Departure_Time_First_Segment) as TravelDays,
                datediff(da.Departure_Time_First_Segment, da.Order_Created) as DaysAhead,
                da.Origin_Airport_ID as OriginAirportID,
                da.Origin_City_ID as OriginCityID,
                da.Origin_Country_ID as OriginCountryID,
                da.Destination_City_ID as DestCityID,
                da.Destination_Country_ID as DestCoutID,
                IF(destcntry.Continent_ID != origcntry.Continent_ID, 1, 0) as IsInterCont,
                da.Journey_Type_ID as JouneyType,
                da.Validating_Carrier_ID as ValidatingCarrier,
                if(da.Majority_marketing_Carrier_ID is not null, da.Majority_marketing_Carrier_ID,-1) as MajMarktCarrier,
                if(da.Majority_Operating_Carrier_ID is not null, da.Majority_Operating_Carrier_ID,-1) as MajOpCarrier,
                if (da.Only_marketing_Carrier_ID is not null, da.Only_marketing_Carrier_ID,-1) as OnlyMarktCarrier,
                if( da.Only_Operating_Carrier_ID is not null, da.Only_Operating_Carrier_ID,-1 ) as OnlyOpCarrier,
                da.Num_Bounds as NumBounds,
                da.Num_Segments as NumSegs,
                da.Bound_2_Start_Segment_1_Based as Bound2StartSeg1Bs,
                da.Num_Search_Adults as NumofAd,
                da.Num_Search_Children as NumofCh,
                da.Num_Search_Infants as Numofinf,
                da.Net,
                da.Net_Tax_Vat as NetTaxVat,
                da.Tax_Excl_Vat as TaxExVat,
                da.Vat,
                da.Markup,
                da.Markup_Segment_ID as MarkupSeg,
                db.Booking_Type_ID as BookType,
                db.First_Day_Of_Service as FirstDayServ,
                db.Last_Day_Of_Service as LastDayServ,
                dor.Markup_Amount,
                dors.Revenue * (1/currconv.exchange_rate) as RevenueSek,
                dors.Marketing_Cost*(1/currconv.exchange_rate) as MarketCostSek,
                (dors.Revenue - dors.Marketing_Cost)* (1/currconv.exchange_rate) as Tb1
                from ibemining.d_air_bookings as da
                left outer join ibemining.d_bookings as db on (da.Order_Created = db.Order_Created and da.Order_ID = db.Order_ID )
                left outer join ibemining.d_order_items as dor on (da.Order_Created = dor.Order_Created and da.Order_ID = dor.Order_ID and  dor.Provider_Item_PCA_ID = da.Provider_Item_PCA_ID)
                left outer join ibemining.d_orders as dors on (da.Order_Created = dors.Created)
                left outer join r_countries destcntry on destcntry.id = da.Destination_Country_ID
                left outer join r_countries origcntry on origcntry.id = da.Origin_Country_ID
                left outer join ibe.currency_conversion as currconv on (dors.Currency_ID = currconv.curr_id_to)
                where (da.Order_Created between '%s' and '%s')
                and (db.Order_Created is not null) and (dor.Order_Created is not null)
                and (dors.Is_Test_Order = 0)
                group by
                OrderTime
                Order by OrderTime ASC""" %(start_date, end_date)
        return self.retrieve_data_with_commands(command=commands)






if __name__=="__main__":
    s = AdvanceDataFetcher()
    list= s.get_attr_list()
    print list["functions"]["datediff"] %( list["Derived_data"]["DaysAhead"]["param"][0], \
                                           list["Derived_data"]["DaysAhead"]["param"][1])
    s.select_attr_append("da.Site_ID")
    s.select_attr_append("da.Num_Bounds")
    print s.get_features()
    s.remove_attr("da.Num_Bounds")
    print s.get_features()
    print s.get_feature_wrapper()
    print s.get_table_tracker()
    s.select_attr_append(func_in="TravelDays", type="Derived")
    s.select_attr_append(func_in="RevenueSek", type="Derived")

    print s.get_features()
    print s.get_feature_wrapper()
    print s.get_table_tracker()


    print s.get_inner_join_wrapper()
    print s.get_aggregation()

    s.get_trip_env_custom(filter=["time"], filter_input=[tuple( ("2012-07-01","2012-07-02") )])