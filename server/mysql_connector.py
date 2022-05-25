# Python implementation of the MySQL connection interface
import pymysql
import configparser
import os


class MysqlConnector(object):

    def __init__(self):
        """
        Constructor for MysqlConnector to initialize the attributes
        """

        # -------------- Read database configuration --------------
        # Detailed configuration in db_config.ini
        root_dir = os.path.dirname(os.path.abspath('.'))
        cf = configparser.ConfigParser()
        cf.read(root_dir + "/server/db_config.ini")

        # ------------- Class variables initialization ----------
        self.__host = cf.get("Database Configuration", "host")
        self.__user = cf.get("Database Configuration", "user")
        self.__passwd = cf.get("Database Configuration", "password")
        self.__database = cf.get("Database Configuration", "database")
        self.__port = int(cf.get("Database Configuration", "port"))
        self.__charset = cf.get("Database Configuration", "charset")

        # ---------- Create database connection connection and cursor ----------
        self.__conn = pymysql.connect(host=self.__host, port=self.__port, database=self.__database, user=self.__user,
                                      passwd=self.__passwd, charset=self.__charset)
        self.__cursor = self.__conn.cursor()

    def __del__(self):
        """
        Database connection close
        """
        self.__conn.close()
        self.__cursor.close()

    def get_connection(self) -> pymysql.cursors:
        """
        Database connection getter

        Returns
        -------
        :return: self.__cursor
        :rtype: pymysql.cursors
        """
        return self.__cursor

    def commit(self):
        """
        Commit executions to database
        """
        self.__conn.commit()
