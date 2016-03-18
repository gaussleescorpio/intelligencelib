from ServerConnectionlocal import Mysql_Functions
import pandas as pd
import sys
import time


class AdvancedDataRetrieval(Mysql_Functions):
    def __init__(self,host_name="bisam-client01", user_name="gauss", pass_word="gauss"):
        Mysql_Functions.__init__(self,hostname=host_name,username=user_name,password=pass_word)
        self.__name = []
        self.__elementcom = None
        self.__tablejoin = None
        self.__scope={"ibemining":["d_orders","d_order_revenue_items", "d_order_items","d_bookings","d_air_bookings"],
                      "ibe":["currency_conversion"]}

    def get_attr(self):
        return self.__name

    def get_scope(self):
        return self.__scope

    def update_scope(self, db_name=None, tb_name=None):
        assert db_name is not None, "please input a db name.."
        assert tb_name is not None, "please input a tb name. "
        self.__scope[db_name].append(tb_name)

    def empty_scope(self):
        self.__scope = {}

    def get_attr_name(self, db_name="ibemining", tb_name="d_air_bookings"):
        commands = """SELECT `COLUMN_NAME`
                     FROM `INFORMATION_SCHEMA`.`COLUMNS`
                     WHERE `TABLE_SCHEMA`='%s'
                     AND `TABLE_NAME`='%s';  """ %(db_name, tb_name)
        name = self.Get_data_with_commands(command=commands)
        return name

    def pd_to_list(self, pddata):
        """
        :param pddata: only one-col dafaframe
        :return:
        """
        assert  isinstance(pddata,pd.DataFrame), "cannot convert non-pandas-dataframe to list"
        if pddata.empty:
            return []
        if pddata.values.shape[0] > pddata.values.shape[1]:
            return pddata.values.T.tolist()[0]
        else:
            return pddata.values.tolist()[0]

    def append_attr(self, name= None, type="scope"):
        assert isinstance(name,list), "Error:please reformat your input data as a list format."
        for target in name:
            # print target
            if target == "Created":
                target = "Order_Created"
            assert isinstance(target,basestring),"there are non-string element in the input, please check.."
            if target not in self.__name:
                if self.if_attr_exist_db("ibemining",target,type) or self.if_attr_exist_db("ibe",target,type):
                    self.__name.append(target)
                else:
                    print "Warning: the attr %s is not listed in any database, it will not be appended" %(target)

    def __define_trip_elements(self, type="scope"):
        # assert isinstance(alias, dict), "please input a dict format..."
        self.__elementcom = "select "
        # for scoping part element SQL code alignment
        if type == "scope":
            temp = []
            if self.__scope["ibemining"]:
                temp = ["%s.%s"%(tb_name,ele)
                        for ele in self.__name
                        for tb_name in self.__scope["ibemining"]
                        if self.if_attr_exist_tb("ibemining",tb_name,ele)]
            if temp:
                last = len(temp) - 1
                for i,s in enumerate(temp):
                    if i == last:
                        self.__elementcom += s + "\n"
                    else:
                        self.__elementcom += s + "," + "\n"
            temp = []
            if self.__scope["ibe"]:
                temp = ["%s.%s"%(tb_name,ele)
                        for ele in self.__name
                        for tb_name in self.__scope["ibe"]
                        if self.if_attr_exist_tb("ibe",tb_name,ele)]
            if temp:
                last = len(temp) - 1
                for i,s in enumerate(temp):
                    if i == last:
                        self.__elementcom += s + "\n"
                    else:
                        self.__elementcom += s + "," + "\n"
            print self.__elementcom
        # for non-scoped part, SQL element alignment
        if type == "all":
            all_tb_names = dict()
            all_tb_names["ibe"] = self.__gettablenames__("ibe")
            all_tb_names["ibemining"] = self.__gettablenames__("ibemining")
            temp = []
            if all_tb_names["ibemining"]:
                temp = ["%s.%s"%(tb_name,ele)
                        for ele in self.__name
                        for tb_name in all_tb_names["ibemining"]
                        if self.if_attr_exist_tb("ibemining",tb_name,ele)]
            if temp:
                last = len(temp) - 1
                for i,s in enumerate(temp):
                    if i == last:
                        self.__elementcom += s + "\n"
                    else:
                        self.__elementcom += s + "," + "\n"
            temp = []
            if all_tb_names["ibe"]:
                temp = ["%s.%s"%(tb_name,ele)
                        for ele in self.__name
                        for tb_name in all_tb_names["ibe"]
                        if self.if_attr_exist_tb("ibe",tb_name,ele)]
            if temp:
                last = len(temp) - 1
                for i,s in enumerate(temp):
                    if i == last:
                        self.__elementcom += s + "\n"
                    else:
                        self.__elementcom += s + "," + "\n"
            print self.__elementcom

            #self.__elementcom += "from ibemining.d_air_bookings as da"


    def if_attr_exist_tb(self, db_name=None, tb_name=None, attr_name=None):
        assert isinstance(tb_name,basestring) and isinstance(attr_name,basestring) \
                and isinstance(db_name, basestring), "check out if your input is a string"
        tb_attr = self.get_attr_name(db_name,tb_name)
        tb_attr = self.pd_to_list(tb_attr)
        if attr_name in tb_attr:
            return True
        else:
            return False

    def if_attr_exist_db(self, db_name=None, attr_name=None, type=None):
        """
        warning: this function might consume a lot of time if your db has a lot of tables, time-complexity O(n^2). Scope type is recommended in this case.
        So use "scope" mode instead of "all".
        :param db_name:
        :param attr_name:
        :param type: "=scope" for limited databased check. Otherwise, check all through database
        :return:
        """
        assert isinstance(attr_name,basestring) \
                and isinstance(db_name, basestring), "check out if your input is a string"
        # print all_tb_names

        if type == "scope":
            if not self.__scope[db_name]:
                return False
            for tb_name in self.__scope[db_name]:
                tb_attr = self.get_attr_name(db_name,tb_name)
                tb_attr = self.pd_to_list(tb_attr)
                if attr_name in tb_attr:
                    return True
            # add another database scope code here
            # for tb_name in self.__scope["ibe"]
        if type == "all":
            all_tb_names = self.__gettablenames__(db_name)
            all_tb_names = self.pd_to_list(all_tb_names)
            for tb_name in all_tb_names:
                tb_attr = self.get_attr_name(db_name,tb_name)
                #print tb_attr
                tb_attr = self.pd_to_list(tb_attr)
                print db_name, tb_name, tb_attr
                if attr_name in tb_attr:
                    return True
        # cannot find it in the whole database
        return False

    def __gettablenames__(self, db_name=None):
        assert isinstance(db_name,basestring), "db_name must be a string"
        commands = """SELECT TABLE_NAME
                     FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='%s'  """ %db_name
        return self.Get_data_with_commands(commands)

    def __define_join_tables(self):

        def __define_jtables_interaction(tg_name):
            while True:
                print "please check table %s if it is properly joined..." % tg_name
                print "please follow the steps below to join table %s querries and always finish with enter:" % tg_name
                temp_str = "left outer join "
                print temp_str + " " + ":input your database_name.table_name.... \n"
                user_input = raw_input()
                temp_str += user_input
                print temp_str + " " + ":input your join col names here, e.g. on db1.Created_ID = db2.Created_ID ... \n"
                user_input = raw_input()
                print "the current command you want to add is: \n " \
                      + temp_str + " " + user_input + "\n" + "Are you sure (y/n)?"
                aff_input = raw_input()
                if aff_input == "y":
                    temp_str += " " + user_input + "\n"
                    break;
                else:
                    print "do you want to redo it?(y/n)"
                    user_input = raw_input()
                    if user_input == "y":
                        continue
                    else:
                        temp_str = ""
                        break
            return temp_str

        self.__tablejoin = """from ibemining.d_air_bookings
                left outer join ibemining.d_bookings on (ibemining.d_air_bookings.Order_Created = ibemining.d_bookings.Order_Created and ibemining.d_air_bookings.Order_ID = ibemining.d_bookings.Order_ID )
                left outer join ibemining.d_order_items on (ibemining.d_air_bookings.Order_Created = ibemining.d_order_items.Order_Created and ibemining.d_air_bookings.Order_ID = ibemining.d_order_items.Order_ID and  ibemining.d_order_items.Provider_Item_PCA_ID = ibemining.d_air_bookings.Provider_Item_PCA_ID)
                left outer join ibemining.d_orders on (ibemining.d_air_bookings.Order_Created = ibemining.d_orders.Created)
                left outer join r_countries destcntry on destcntry.id = ibemining.d_air_bookings.Destination_Country_ID
                left outer join r_countries origcntry on origcntry.id = ibemining.d_air_bookings.Origin_Country_ID
                left outer join ibe.currency_conversion  on (ibemining.d_orders.Currency_ID = ibe.currency_conversion.curr_id_to) \n """
        list = []
        for s in self.__scope.values():
            list += s
        for tg_name in list:
            if tg_name not in self.__tablejoin:
                r_string = __define_jtables_interaction(tg_name)
                if r_string:
                    self.__tablejoin += r_string

    def get_trip_env_custom(self, start_date="2012-07-01", end_date="2012-07-02", type="scope"):
        def __define_sql_wheregroupby(existing_sql=None):
            assert existing_sql is not None, "please make sure you have already had sql commands...."
            print "the current sql is: \n" + self.__elementcom + self.__tablejoin
            time.sleep(2)
            print "now we start up to where and group by info and finish the input by pressing enter.... " \
                  + "(skip by pressing enter)"
            temp_str = "where "
            while True:
                print "where : add condition except time limitation here... eg. (condition 1) and (condition2) and..."
                user_input = raw_input()
                if user_input:
                    print "where " + user_input + "\n" \
                            + "confirm? (y/n)"
                    aff_input = raw_input()
                    if aff_input == "y":
                        temp_str += user_input + "\n"
                        break
                    else:
                        print "want to redo it? (y/n) \n"
                        aff1_input = raw_input()
                        if aff1_input == "y":
                            continue
                        else:
                            break
                else:
                    print "you skipped the input for where...."
                    break
            temp_str += """ and (ibemining.d_air_bookings.Order_Created between '%s' and '%s') """ % (start_date, end_date)
            while True:
                print "group by : add attribute name here... eg.  attr1, attr2, ..."
                user_input = raw_input()
                if user_input:
                    print "group by " + user_input + "\n" \
                            + "confirm? (y/n)"
                    aff_input = raw_input()
                    if aff_input == "y":
                        temp_str += "group by " + user_input + "\n"
                        break
                    else:
                        print "want to redo it? (y/n) \n"
                        aff1_input = raw_input()
                        if aff1_input == "y":
                            continue
                        else:
                            break
                else:
                    print "you skipped input for group by"
                    break

            return temp_str

        self.__define_trip_elements(type)
        self.__define_join_tables()
        end_string = __define_sql_wheregroupby(self.__elementcom + self.__tablejoin)
        print "##############################"*10
        print "The final sql command is:"
        print self.__elementcom + self.__tablejoin + end_string
        return self.Get_data_with_commands(self.__elementcom + self.__tablejoin + end_string)

        # if type == "scope":
        #     # i = len(self.__scope["ibemining"])
        #     for i, s in enumerate(self.__scope["ibemining"]):
        #         if s == "d_air_bookings":
        #             continue
        #         if self.__tablejoin is None:
        #             self.__tablejoin =""
        #         self.__tablejoin += """ left outer join ibemining.%s
        #                            on (%s.%s = %s.%s and  = %s.%s) """
        #                         %()
        #
        #
        # if type=="all":
        #     print "in development....."

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
        return self.get_data_with_commands(command=commands)












if __name__=="__main__":
    mm = AdvancedDataRetrieval()
    name = mm.get_attr_name()
    name = mm.pd_to_list(name)
    mm.append_attr(name)
    print len( mm.get_attr() )
    # name = mm.get_attr_name(db_name="ibemining", tb_name="d_orders")
    # mm.append_attr(mm.pd_to_list(name))
    # name = mm.get_attr_name(db_name="ibemining", tb_name="d_order_items")
    # mm.append_attr(mm.pd_to_list(name))
    # print len(mm.get_attr())
    print mm.get_attr()
    mm.Use_database("ibemining")
    # print mm.get_trip_env_default(start_date="2012-08-01", end_date="2012-08-02")
    print mm.get_trip_env_custom()







