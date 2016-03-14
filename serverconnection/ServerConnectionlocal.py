"""
This module aims to define a fundamental functions for connecting and retrieving data from bisam_client01 database.
Author: Gauss Lee
Date : 2016-03-01

Module description:
Users can use this module to connect to bisam-client01 server by initilizing the data retrival process. Base virutal class
, derived data retrival class and implementation examples are all given below.

Some heads-up for using this module:
1. for retrieving all of the data, you should use function read_data_by_selection.
2. for retrieving data with known indexes, you should use function read_data_by_selection_index. The index can be sql datatime, such as
   a string "2013-01-01" or a int index which is used to quote different rows in the sql database.
3. for getting heads and tails of the data from a particular table. You can use Read_data_head or Read_data_tails. Please note that these two functions
    might be slow if you want to retieve a lot of data.
4. For any more complicated mysql commands, you can use get_data_with_commands for general purposes.

In the next version, it will be updated to use ibis and impala.
"""

import MySQLdb as mdb
import pandas as pd
from pandas.io import sql
import abc

# hostname = "bisam-client01"
# username = "gauss"
# password = "gauss"

# db = mdb.connect(host=hostname, user=username, passwd=password)
# cursor = db.cursor()

class Virtual_Mysql_Functions(object):
    """
    virtual class for all users. Only provide a format of intended classes. Any pure vitural function should have an implementation in
    the derived class. Otherwise, compile errors!!
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def Use_database(self):
        """
        This is the pure virtual function for determining which database you have to use
        and it is not supposed to be instantiatialized
        :return:
        """
        pass
    @abc.abstractmethod
    def Read_data_by_selection(self):
        """
        This it the pure virtual function for retriving data from mysql database without any
        selection limits.
        :return:
        """
        pass





class  Mysql_Functions(Virtual_Mysql_Functions):
    """
    This class mainly works for one single database, if you wanna information across different databases, please use the "get data with commands"
    function by specifying more complicated mysql commands.
    """
    def __init__(self,hostname="bisam-client01", username="gauss", password="gauss"):
        """
        Init the mysql server connection
        """
        self.__remote_hostname = hostname

        self.__remote_username = username
        self.__password = password
        con_success = True
        try:
            self.db = mdb.connect(host=hostname, user=username,passwd=password)
        except mdb.OperationalError as e:
            con_success = False
            print "ERROR: %s" %(str(e))
            print "connection failed....... Please check the server availibilities and your local server name."
        finally:
            if con_success:
                print "connection to Mysql server %s is succesful...." %(self.__remote_hostname)
                self.cur = self.db.cursor()
        self.database = None

    def Use_database(self,db_name="default"):

        """
        Is to choose a table to use from remote mysql database.
        :param tb_name:  table name
        :return:
        """

        assert isinstance(db_name,basestring) , "please input a string..."
        try:
            fb = self.cur.execute( "USE %s "%(db_name) )
            self.database_name = db_name
        except self.db.OperationalError as err:
            print "ERROR: %s" %(err)



    def if_database_exist(self, db_name = "d"):
        """
        Check it database contains a certain table
        :param db_name: database name 
        :return: Ture if the database exists otherwise False
        """
        assert isinstance(db_name,basestring) , "please input a string..."
        exist_con = self.cur.execute( " SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s' " %(db_name) )
        if exist_con:
            return True
        else:
            return False

    def if_table_exist(self, tb_name):
        """
        check if current table is availiable in the used database
        :param tb_name:
        :return:
        """
        assert isinstance(tb_name, basestring), "please input a string for table name"
        exist_con = self.cur.execute(" show tables like '%s' " %(tb_name))
        return exist_con


    def Read_data_by_selection(self, con="*", tb_name=None):
        """
        This function is used to access the database with small size. For big size data retrieval, please use Read_data_by_selection_index
        :param con: a string composed of all the retrieving attrs or cols, ex: "attr1, attr2"
        :param tb_name: denotes which table it is
        :return: data retrieved. otherwise -1
        """
        dict_rec = pd.DataFrame({})
        #judge col names of table you want to retrieve
        if not con:
            print "nothing to read from database"
            return -1
        else:
            # make sure the database is okay and table exists
            if self.database_name is not None and self.if_table_exist(tb_name):
                #num_of_entries = len(con)
                #access the data and retrieve:
                dict_rec = sql.read_sql( """select %s from %s """ %(con, tb_name), con=self.db)
                return dict_rec
            else:
                print "database should be firstly set and table name %s should exist..."%(tb_name)
                return -1

    def Read_data_by_selection_index(self,con="*",tb_name=None,start_index=None, end_index=None,index_name="Default"):
        """
        This function is used to access database by setting up a particular scope with index (e.g. datatime)
        :param con: a string composed of all the retrieving attrs or cols, ex: "attr1, attr2"
        :param tb_name:denotes which table it is
        :param start_index: str means datetime "2014-02-01" or int means index
        :param end_index: str means datetime "2014-02-01" or int means index
        :return:
        """
        assert isinstance(index_name, basestring), "please input a valid index name"
        dict_rec = pd.DataFrame({})
        if not con:
            print "nothing to read from database"
            return -1
        else:
            # make sure the database is okay and table exists
            if self.database_name is not None and self.if_table_exist(tb_name):
                #num_of_entries = len(con)
                #access the data and retrieve
                if isinstance(start_index,basestring) and isinstance(end_index, basestring):
                    dict_rec = sql.read_sql( """select %s from %s where %s between '%s' and '%s' order by %s ASC """ \
                                           %(con, tb_name, index_name,start_index,end_index,index_name),
                                           con=self.db)
                else:
                    dict_rec = sql.read_sql( """select %s from %s where %s between %s and %s order by %s ASC""" \
                                           %(con, tb_name, index_name,start_index,end_index,index_name),
                                           con=self.db)
                #retrive data only within index range
                #dict_rec = dict_rec[ dict_rec.index.isin( range(start_index, end_index+1) ) ]
                return dict_rec
            else:
                print "database should be firstly set and table name %s should exist..."%(tb_name)
                return -1

    def Read_data_head(self,con="*", num=1, tb_name=None):
        """
        Do not assign very big num with huge tables to this function, it will be slow
        :param con:  a string composed of all the retrieving attrs or cols, ex: "attr1, attr2"
        :param num: num of head elements you want to get
        :return:
        """
        commands = """ select %s from %s  """ %(con,tb_name)
        dict_rec = self.Get_data_with_commands(commands)
        return dict_rec.head(num)

    def Read_data_tails(self, con="*", num = 1, tb_name=None):
        """
        Do not assign very big num with huge tables to this function, it will be slow
        :param con:  a string composed of all the retrieving attrs or cols, ex: "attr1, attr2"
        :param num: num of head elements you want to get
        :param tb_name:
        :return:
        """
        commands = """ select %s from %s  """ %(con,tb_name)
        dict_rec = self.Get_data_with_commands(commands)
        return dict_rec.tail(num)

    def Get_data_with_commands(self, command=""):
        assert isinstance(command, basestring), "please input sql commands as string type"
        rec = -1
        try:
            rec = sql.read_sql(command, con=self.db)
        except pd.sql.DatabaseError as e1:
            print "sql command error: %s"%str(e1)
        finally:
            if rec is -1:
                #print "there is nothing to read... check out your table name and col name"
                raise AssertionError("there is nothing to read... check out your table name and col name")
            return rec








if __name__ == "__main__":
    server = Mysql_Functions()
    server.Use_database(db_name="ibemining")
    print server.if_table_exist("d_air_bookings")
    a = server.Read_data_by_selection("NAME,CITY_ID",tb_name="r_airports")

    print server.Read_data_by_selection_index(con="Order_ID",tb_name="d_air_bookings",start_index ="2014-02-01",end_index="2014-03-01",index_name="Order_Created" )
    print server.Get_data_with_commands(command="""select NAME from r_airports""")
    #commands = """select Order_Created, Departure_Time_First_Segment from d_air_bookings where Order_Created between '2016-03-01' and '2016-03-03' """
    #print server.Get_data_with_commands(command=commands)
    print server.Read_data_head(tb_name="r_airports",con="NAME,CITY_ID",num=100)
    print server.Read_data_tails(tb_name="r_airports",con="NAME,CITY_ID",num=100)