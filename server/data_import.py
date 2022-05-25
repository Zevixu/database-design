import csv
import mysql_connector as sqlc
import pymysql
import datetime


def data_import(csv_name, cursor, cmd, connector):
    """
    load data from local to server
    Parameters
    ----------
    csv_name: .csv file name
    cursor: mysql connection cursor object
    cmd: SQL command to execute

    """
    with open('./csv/{c}'.format(c=csv_name), newline='', encoding="UTF-8") as file:
        load = csv.reader(file, delimiter=',')
        first_row = True
        count = 0
        for row in load:
            # ignore 1 line
            if first_row:
                first_row = False
                continue
            # for the airport_weather.csv file: change str to date
            if csv_name == "airport_weather.csv":
                try:
                    row[2] = datetime.datetime.strptime(row[2], '%m/%d/%Y').date()
                except ValueError:
                    continue
            #  Missing data set to NULL
            for i in range(0, len(row)):
                if row[i] == '':
                    row[i] = None
            try:
                count += 1
                cursor.execute(cmd, row)
                if count == 100:
                    count = 0
                    connector.commit()
            except (pymysql.Error, TypeError):
                continue


def import_loop():
    """
    Create SQL statements of each table in the database
    Call data_import for each table to load data
    -------

    """
    mysql_connector = sqlc.MysqlConnector()
    cursor = mysql_connector.get_connection()
    load_cmd = {}

    # Table AirportList load data command
    load_cmd[
        "airports_list"] = "INSERT INTO AirportList " \
                           "(ORIGIN_AIRPORT_ID,DISPLAY_AIRPORT_NAME,ORIGIN_CITY_NAME,NAME) " \
                           "VALUES (%s, %s, %s, %s);"

    # Table Aircraft_Inventory load data command
    load_cmd[
        "AIRCRAFT_INVENTORY"] = "INSERT INTO Aircraft_Inventory " \
                                "(MANUFACTURE_YEAR,TAIL_NUM,NUMBER_OF_SEATS) " \
                                "VALUES (%s, %s, %s);"

    # Table CarrierDecode load data command
    load_cmd[
        "CarrierDecode"] = "INSERT INTO CarrierDecode " \
                           "(AIRLINE_ID,OP_UNIQUE_CARRIER,CARRIER_NAME) " \
                           "VALUES (%s, %s, %s);"

    # Table AirportCoordinates load data command
    load_cmd[
        "AirportCoordinates"] = "INSERT INTO AirportCoordinates " \
                                "(ORIGIN_AIRPORT_ID,LATITUDE,LONGITUDE) " \
                                "VALUES (%s, %s, %s);"

    # Table Airport_Weather load data command
    load_cmd[
        "Airport_Weather"] = "INSERT INTO Airport_Weather " \
                             "(ORIGIN_AIRPORT_ID,STATION,Weather_DATE,AWND, " \
                             "PGTM, PRCP, SNOW, SNWD, TAVG, TMAX, TMIN, WDF2, WDF5, WSF2, WSF5, " \
                             "WT01, WT02, WT03, WT04, WT05, WT06, WT07, WT08, WT09, WESD, WT10, " \
                             "PSUN, TSUN, SN32, SX32, TOBS, WT11, WT18)" \
                             "VALUES (%s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s);"

    # Table CARRIER_SUMMARY_AIRPORT_ACTIVITY load data command
    load_cmd[
        "AIRPORT_ACTIVITY"] = "INSERT INTO CARRIER_SUMMARY_AIRPORT_ACTIVITY " \
                              "(OP_UNIQUE_CARRIER,CARRIER_NAME,ORIGIN_AIRPORT_ID,SERVICE_CLASS,REV_ACRFT_DEP_PERF_510,REV_PAX_ENP_110) " \
                              "VALUES (%s, %s, %s,%s, %s, %s);"

    # Table Employees load data command
    load_cmd[
        "Employees"] = "INSERT INTO Employees " \
                       "(YEAR,AIRLINE_ID,OP_UNIQUE_CARRIER,CARRIER_NAME,ENTITY,GENERAL_MANAGE,PILOTS_COPILOTS," \
                       "OTHER_FLT_PERS,PASS_GEN_SVC_ADMIN,MAINTENANCE,ARCFT_TRAF_HANDLING_GRP1,GEN_ARCFT_TRAF_HANDLING," \
                       "AIRCRAFT_CONTROL,PASSENGER_HANDLING,CARGO_HANDLING,TRAINEES_INTRUCTOR,STATISTICAL," \
                       "TRAFFIC_SOLICITERS,OTHER,TRANSPORT_RELATED,TOTAL) " \
                       "VALUES (%s, %s, %s,%s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s);"

    # -----------  call data_import function to load data to server ------------------
    # data_import("airports_list.csv", cursor, load_cmd["airports_list"], mysql_connector)
    # data_import("AIRCRAFT_INVENTORY.csv", cursor, load_cmd["AIRCRAFT_INVENTORY"], mysql_connector)
    # data_import("CARRIER_DECODE.csv", cursor, load_cmd["CarrierDecode"], mysql_connector)
    # data_import("AIRPORT_COORDINATES.csv", cursor, load_cmd["AirportCoordinates"], mysql_connector)
    data_import("airport_weather.csv", cursor, load_cmd["Airport_Weather"], mysql_connector)
    # data_import("AIRPORT_ACTIVITY.csv", cursor, load_cmd["AIRPORT_ACTIVITY"], mysql_connector)
    # data_import("EMPLOYEES.csv", cursor, load_cmd["Employees"], mysql_connector)
    mysql_connector.commit()

    # -----------  call data_import function to load data to server for table ONTIME_REPORTING------------------
    # Table AirportCoordinates load data command
    # load_cmd[
    #     "AirportCoordinates"] = "INSERT INTO AirportCoordinates " \
    #                             "(ORIGIN_AIRPORT_ID,LATITUDE,LONGITUDE) " \
    #                             "VALUES (%s, %s, %s);"


if __name__ == '__main__':
    import_loop()
