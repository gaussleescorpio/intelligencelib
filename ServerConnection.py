import MySQLdb as mdb
import pandas as pd
from pandas.io import sql

# hostname = "bisam-client01"
# username = "gauss"
# password = "gauss"

# db = mdb.connect(host=hostname, user=username, passwd=password)
# cursor = db.cursor()

class  Mysql_Functions:
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


    def Read_data_by_selection(self, con=[], tb_name=None):
        """
        This function is used to access the database with small size. For big size data retrieval, please use Read_data_by_selection_index
        :param con: a list of all the table cols you like to retrieve
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
                #access the data and retrieve
                for name in con:
                    dict_rec[name] = sql.read_sql( "select %s from %s " %(name, tb_name), con=self.db)
                return dict_rec
            else:
                print "database should be firstly set and table name %s should exist..."%(tb_name)
                return -1

    def Read_data_by_selection_index(self,con=[],tb_name=None,start_index=None, end_index=None):
        """
        This function is used to access database by setting up a particular scope with index (e.g. datatime)
        :param con: a list of all the table cols you like to retrieve
        :param tb_name:denotes which table it is
        :param start_index:
        :param end_index:
        :return:
        """
        dict_rec = pd.DataFrame({})
        if not con:
            print "nothing to read from database"
            return -1
        else:
            # make sure the database is okay and table exists
            if self.database_name is not None and self.if_table_exist(tb_name):
                #num_of_entries = len(con)
                #access the data and retrieve
                for name in con:
                    dict_rec[name] = sql.read_sql( "select %s from %s " %(name, tb_name), con=self.db)
                #retrive data only within index range
                dict_rec = dict_rec[ dict_rec.index.isin( range(start_index, end_index+1) ) ]
                return dict_rec
            else:
                print "database should be firstly set and table name %s should exist..."%(tb_name)
                return -1

    def Get_data_with_commands(self, command=""):
        assert isinstance(command, basestring), "please input sql commands as string type"
        rec = -1
        try:
            rec = sql.read_sql(command, con=self.db)
        except self.db.OperationalError as e1:
            print "sql command error: %s"%str(e1)
        finally:
            if rec is -1:
                print "there is nothing to read... check out your table name and col name"
            return rec






if __name__ == "__main__":
    server = Mysql_Functions()
    server.Use_database(db_name="ibemining")
    print server.if_table_exist("d_air_bookings")
    a = server.Read_data_by_selection(["NAME","CITY_ID"],tb_name="r_airports")

    print server.Read_data_by_selection_index(con=["NAME","CITY_ID"],tb_name="r_airports",start_index =2,end_index=50 )
    print server.Get_data_with_commands(command="""select NAME from r_airports""")
    commands = """select Order_Created, Departure_Time_First_Segment from d_air_bookings where Order_Created between '2016-03-01' and '2016-03-03' """
    print server.Get_data_with_commands(command=commands)
