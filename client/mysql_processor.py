import pymysql
import yaml
import sys


class Processor(object):

    def __init__(self):
        """
        Constructor for MysqlProcessor to read SQL statements from file "sql_statement.yaml"
        """
        file = open("sql_statement.yaml", encoding="utf-8")
        self.sql_statement = yaml.full_load(file)

    def do_query(self, connector, query_params):
        """
        Query operation
        """

        # -------------- initialize variables --------------
        # nc_query: not cancelled flights delay time query
        # c_query: cancelled flights delay counts query
        sql_statement = {}
        # -------------- extract query parameters --------------
        cursor = connector.get_connection()
        origin_city = query_params["oc"]
        destination_city = query_params["dc"]
        month = query_params["month"]
        date = query_params["date"]
        date_week = query_params["dw"]
        if month != "NULL":
            month = int(month)
        if date != "NULL":
            date = int(date)
        if date_week != "NULL":
            date_week = int(date_week)
        flight_num = query_params["flight_num"]
        output_month = ["", "January", "February", "March", "April", "May", "June", "July", "August", "September",
                        "October", "November", "December"]
        output_day_week = ["", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        # -------------- decide which SQL statement should be executed--------------
        def case_switch(mm, dd, dw):
            if mm != "NULL":
                if dd != "NULL":
                    return 2
                else:
                    return 1
            else:
                if date_week != "NULL":
                    return 3
                else:
                    return 0

        case_num = case_switch(month, date, date_week)

        # -------------- SQL execution function --------------
        def statement_execution(do_cursor, sql):
            # -------------- execute SQL statements and store the results --------------
            pre_check = sql["0"]
            try:
                do_cursor.execute(pre_check)
            except pymysql.Error as n:
                print(n)
                sys.exit(2)
            result_dict = {}
            connector.commit()
            result_nc = cursor.fetchone()
            if do_cursor.rowcount == 0 or result_nc[0] == 0:
                result_dict["0"] = (
                    "1. The query for flights data returned 0 rows:"
                    "No flight delay data in the database under your input "
                    "parameters.")
            else:
                count = result_nc[0]
                result_dict["0"] = "{count} flight(s) found in the database: ".format(count=count)

                for i in range(1, 8):
                    do_cursor.execute(sql["{num}".format(num=i)])
                    connector.commit()
                    query_result = cursor.fetchone()

                    if i == 1:
                        percent = "{:.0%}".format(int(query_result[0]) / int(count))
                        result_dict[
                            "{num}".format(
                                num=i)] = "\t1. {How_many} flight(s) ({percent}) departed without delay.".format(
                            How_many=query_result[0], percent=percent)

                    elif i == 2:
                        percent = "{:.0%}".format(int(query_result[0]) / int(count))
                        result_dict[
                            "{num}".format(
                                num=i)] = "\t2. {How_many} flight(s) ({percent}) arrived without delay.".format(
                            How_many=query_result[0], percent=percent)

                    elif i == 3:
                        percent = "{:.0%}".format(int(query_result[0]) / int(count))
                        result_dict["{num}".format(
                            num=i)] = "\t3. {How_many} flight(s) ({percent}) were not delayed for departure and arrival.".format(
                            How_many=query_result[0], percent=percent)

                    elif i == 4:
                        count_4 = query_result[0]
                        if query_result[1] is None:
                            avg_delay_dep = 0
                        else:
                            avg_delay_dep = format(query_result[1], '.2f')
                        if query_result[2] is None:
                            max_delay_dep = 0
                        else:
                            max_delay_dep = format(query_result[2], '.2f')
                        percent = "{:.0%}".format(int(count_4) / int(count))
                        result_dict["{num}".format(
                            num=i)] = "\t4. {count} flight(s) ({percent}) were delayed for departure:\n" \
                                      "\t\taverage delay {avg_delay_dep} minutes, " \
                                      "maximum delay {max_delay_dep} minutes.".format(
                            count=count_4, avg_delay_dep=avg_delay_dep, max_delay_dep=max_delay_dep, percent=percent)

                    elif i == 5:
                        count_5 = query_result[0]
                        if query_result[1] is None:
                            avg_delay_arr = 0
                        else:
                            avg_delay_arr = format(query_result[1], '.2f')
                        if query_result[2] is None:
                            max_delay_arr = 0
                        else:
                            max_delay_arr = format(query_result[2], '.2f')
                        percent = "{:.0%}".format(int(count_5) / int(count))
                        result_dict[
                            "{num}".format(num=i)] = "\t5. {count} flight(s) ({percent}) were delayed for arrival:\n" \
                                                     "\t\tAverage delay {avg_delay_arr} minutes, " \
                                                     "maximum delay {max_delay_arr} minutes.".format(
                            count=count_5, avg_delay_arr=avg_delay_arr, max_delay_arr=max_delay_arr, percent=percent)

                    elif i == 6:
                        count_6 = query_result[0]
                        if query_result[1] is None:
                            avg_delay_dep = 0
                        else:
                            avg_delay_dep = format(query_result[1], '.2f')
                        if query_result[2] is None:
                            avg_delay_arr = 0
                        else:
                            avg_delay_arr = format(query_result[2], '.2f')
                        if query_result[3] is None:
                            max_delay_dep = 0
                        else:
                            max_delay_dep = format(query_result[3], '.2f')
                        if query_result[4] is None:
                            max_delay_arr = 0
                        else:
                            max_delay_arr = format(query_result[4], '.2f')

                        percent = "{:.0%}".format(int(count_6) / int(count))
                        result_dict["{num}".format(num=i)] = (
                            "\t6. {count} flight(s) ({percent}) had delayed departures and arrivals: \n"
                            "\t\tAverage departure delay {avg_delay_dep} minutes, "
                            "average arrival delay {avg_delay_arr} minutes.\n"
                            "\t\tMaximum departure delay {max_delay_dep} minutes, "
                            "maximum arrival delay {max_delay_arr} minutes.".format(
                                count=count_6,
                                avg_delay_dep=avg_delay_dep,
                                avg_delay_arr=avg_delay_arr,
                                max_delay_dep=max_delay_dep,
                                max_delay_arr=max_delay_arr,
                                percent=percent))

                    elif i == 7:
                        if do_cursor.rowcount == 0 or query_result[0] == 0:
                            result_dict["{num}".format(num=i)] = (
                                "\t7. No flight cancellation data in the database under "
                                "your input parameters.")
                        else:
                            count_7 = query_result[0]
                            percent = "{:.0%}".format(int(count_7) / int(count))
                            result_dict["{num}".format(num=i)] = (
                                "\t7. {count} flight(s) ({percent}) cancelled".format(count=count_7, percent=percent))

            for key in result_dict:
                print(result_dict[key])

        # -------------- SQL statements build --------------
        # -------------------------------------------------------
        # Query the delay data of a specific flight (with or without the time data)
        #         1.  flightNo
        #         2.  flightNo + month
        #         3.  flightNo + month + date
        #         4.  flightNo + date_week
        # -------------------------------------------------------
        # Query the delay data of the flights from the origin city (with or without the time data)
        #         1. origin_city
        #         2. origin_city + month
        #         3. origin_city + month + date
        #         4. origin_city + date_week
        # -------------------------------------------------------
        # 	Query the delay data of the flights to the destination city (with or without the time data)
        #         1. destination_city
        #         2. destination_city + month
        #         3. destination_city + month + date
        #         4. destination_city + date_week
        # -------------------------------------------------------
        # 	Query the delay data of the flights from the origin city to the destination city (with or without the time data)
        #         1. origin_city + destination_city
        #         2. origin_city + destination_city + month
        #         3. origin_city + destination_city + month + date
        #         4. origin_city + destination_city + date_week
        # -------------------------------------------------------

        if flight_num != "NULL":
            if case_num == 0:
                print("Delay data of flight No.{num} in 2019:".format(num=flight_num))
                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query40_0"] % (flight_num, 1)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query40_{n}".format(n=i)] % (
                            flight_num, 0)

            elif case_num == 1:
                print("Delay data of flight No.{num} in {month} 2019:".format(num=flight_num,
                                                                              month=output_month[month]))
                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query41_0"] % (flight_num, 1, month)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query41_{n}".format(n=i)] % (
                            flight_num, 0, month)

            elif case_num == 2:
                print("Delay data of flight No.{num} on {month} {date} 2019:".format(num=flight_num,
                                                                                     month=output_month[
                                                                                         month],
                                                                                     date=date))
                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query42_0"] % (flight_num, 1, month, date)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query42_{n}".format(n=i)] % (
                            flight_num, 0, month, date)

            elif case_num == 3:
                print("Delay data of flight No.{num} every {dw} in 2019:".format(num=flight_num,
                                                                                 dw=output_day_week[
                                                                                     date_week]))

                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query43_0"] % (flight_num, 1, date_week)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query43_{n}".format(n=i)] % (
                            flight_num, 0, date_week)

        #  -------------- without flight number: query with origin and/or destination ---------------
        else:
            if origin_city != "NULL" and destination_city == "NULL":
                origin_city_regex = "^" + origin_city
                destination_city_regex = ".*"
            elif origin_city == "NULL" and destination_city != "NULL":
                origin_city_regex = ".*"
                destination_city_regex = "^" + destination_city
            elif origin_city != "NULL" and destination_city != "NULL":
                origin_city_regex = "^" + origin_city
                destination_city_regex = "^" + destination_city
            else:
                print("Input arguments error: must have origin and/or destination")
                sys.exit(2)

            if case_num == 0:
                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query10_0"] % (
                            origin_city_regex, destination_city_regex, 1)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query10_{n}".format(n=i)] % (
                            origin_city_regex, destination_city_regex, 0)

            elif case_num == 1:
                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query11_0"] % (
                            origin_city_regex, destination_city_regex, 1, month)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query11_{n}".format(n=i)] % (
                            origin_city_regex, destination_city_regex, 0, month)

            elif case_num == 2:
                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query12_0"] % (
                            origin_city_regex, destination_city_regex, 1, month, date)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query12_{n}".format(n=i)] % (
                            origin_city_regex, destination_city_regex, 0, month, date)

            elif case_num == 3:
                sql_statement = {}
                for i in range(0, 8):
                    if i == 7:
                        sql_statement["7"] = self.sql_statement["nc_query13_0"] % (
                            origin_city_regex, destination_city_regex, 1, date_week)
                    else:
                        sql_statement["{n}".format(n=i)] = self.sql_statement["nc_query13_{n}".format(n=i)] % (
                            origin_city_regex, destination_city_regex, 0, date_week)

        # -------------- SQL execution --------------
        try:
            statement_execution(cursor, sql_statement)
        except pymysql.Error as n:
            print(n)

    def do_modify(self, connector, modify_params):
        # modify
        # month + date + origin_city + destination_city + flightNumber

        # -------------- extract query parameters --------------
        cursor = connector.get_connection()
        month = modify_params["month"]
        date = modify_params["date"]
        if month != "NULL":
            month = int(month)
        if date != "NULL":
            date = int(date)
        origin_city = modify_params["oc"]
        destination_city = modify_params["dc"]
        flight_num = int(modify_params["flight_num"])
        origin_city_regex = "^" + origin_city
        destination_city_regex = "^" + destination_city

        # -------------- run sql to check if can locate the input item --------------
        sql = self.sql_statement["m_statement_pre"]
        try:
            cursor.execute(sql, (origin_city_regex, destination_city_regex, month, date, flight_num))
            connector.commit()
        except pymysql.Error as n:
            print(n)

        # -------------- input params cannot locate a specific item--------------
        if cursor.rowcount != 1:
            print(
                "Unable to locate the specific input flight. Please use [-q] argument to query the database to check "
                "your input")
            sys.exit(2)

        # --------------  a specific item can be located by input params --------------
        while True:
            cancel = input('Is this flight cancelled? (yes/no)')

            # Flight is cancelled.
            # All delay data update to NULL.
            # ALL cancel data updated
            if cancel == "yes":
                cancellation_code = input('CANCELLATION_CODE [A-Z]:')
                sql = self.sql_statement["m_statement_c"]
                try:
                    cursor.execute(sql, (
                        cancellation_code, month, date, flight_num, origin_city_regex, destination_city_regex))
                    connector.commit()
                except pymysql.Error as n:
                    print(n)
                    sys.exit(2)

                print("Modify completed")
                sys.exit(0)

            # Flight is not cancelled
            # All delay data to be updated
            elif cancel == "no":
                pre_result = cursor.fetchone()
                CRS_DEP_TIME = int(pre_result[12])
                DEP_TIME = int(input('Actual departure time:'))
                DEP_DELAY_NEW = 0
                DEP_DEL15 = 0
                if DEP_TIME > CRS_DEP_TIME:
                    DEP_DELAY_NEW = int(DEP_TIME) - int(CRS_DEP_TIME)
                    if DEP_DELAY_NEW > 15:
                        DEP_DEL15 = 1
                CRS_ARR_TIME = int(pre_result[17])
                ARR_TIME = int(input('Actual arrival time:'))
                ARR_DELAY_NEW = 0
                if ARR_TIME > CRS_ARR_TIME:
                    ARR_DELAY_NEW = int(ARR_TIME) - int(CRS_ARR_TIME)
                CARRIER_DELAY = input('Flag for a carrier delay(0/1):')
                WEATHER_DELAY = input('Flag for a weather delay(0/1):')
                NAS_DELAY = input('Flag for a NAS delay(0/1):')
                SECURITY_DELAY = input('Flag for a security delay(0/1):')
                LATE_AIRCRAFT_DELAY = input('Flag for a late aircraft delay(0/1):')

                sql = self.sql_statement["m_statement_nc"]
                try:
                    cursor.execute(sql, (
                        DEP_TIME, DEP_DELAY_NEW, DEP_DEL15, ARR_TIME, ARR_DELAY_NEW, CARRIER_DELAY, WEATHER_DELAY,
                        NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, month, date, flight_num, origin_city_regex,
                        destination_city_regex))
                    connector.commit()
                except pymysql.Error as n:
                    print(n)
                    sys.exit(2)

                connector.commit()
                print("Modify completed")
                sys.exit(0)

            else:
                print("input error: %s" % cancel)

    def do_delete(self, connector, delete_params):
        """
        Delete operation
        """

        # -------------- extract query parameters --------------
        cursor = connector.get_connection()
        month = delete_params["month"]
        date = delete_params["date"]
        origin_city = delete_params["oc"]
        destination_city = delete_params["dc"]
        flight_num = delete_params["flight_num"]
        origin_city_regex = "^" + origin_city
        destination_city_regex = "^" + destination_city

        # -------------- execute SQL --------------
        sql = self.sql_statement["do_delete"]
        try:
            cursor.execute(sql, (month, date, flight_num, origin_city_regex, destination_city_regex))
            connector.commit()
        except pymysql.Error as n:
            print(n)

        # -------------- input params cannot locate a specific item--------------
        if cursor.rowcount != 1:
            print(
                "Unable to locate the specific input flight. Please use [-q] argument to query the database to check "
                "your input")
            sys.exit(2)
        else:
            print("Delete completed")
            sys.exit(0)

    def do_insert(self, connector, insert_params):
        """
        Insert operation
        """

        # -------------- execute SQL --------------
        cursor = connector.get_connection()
        sql = self.sql_statement["do_insert"]
        try:
            # TODO: check params format match SQL format
            cursor.execute(sql, (
                insert_params[0], insert_params[1], insert_params[2], insert_params[3], insert_params[4],
                insert_params[5],
                insert_params[6], insert_params[7], insert_params[8], insert_params[9], insert_params[10],
                insert_params[11], insert_params[12], insert_params[13], insert_params[14],
                insert_params[15],
                insert_params[16], insert_params[17], insert_params[18], insert_params[19], insert_params[20],
                insert_params[21], insert_params[22], insert_params[23], insert_params[24],
                insert_params[25],
                insert_params[26], insert_params[27], insert_params[28], insert_params[29], insert_params[30],
                insert_params[31]))
            connector.commit()
        except pymysql.Error as n:
            print(n)

        # -------------- input params cannot locate a specific item--------------
        if cursor.rowcount != 1:
            print(
                "Insert error: check input parameters format")
            sys.exit(2)
        else:
            print("Insert completed")
            sys.exit(0)

    def query_print(self, columns, cursor):
        """
        Print query results

        Parameters
        ----------
        columns: query result columns
        cursor: mysql connection cursor

        """
        print("Return {itemNo} query result(s):".format(itemNo=cursor.rowcount))
        count = 0
        while True:
            if count == cursor.rowcount:
                break
            print("Result {n}:".format(n=(count + 1)))
            result = cursor.fetchone()
            result_str = ""
            for r in range(0, len(result)):
                tmp = columns[r] + ": " + str(result[r]) + "\t"
                result_str += tmp
                if (r + 1) % 1 == 0:
                    result_str += "\n"

            print(result_str)
            print()
            count += 1

    def do_employee(self, connector, airline):
        """
        employee table query operation
        """

        # -------------- execute SQL --------------
        cursor = connector.get_connection()
        sql = self.sql_statement["employ_query"]

        try:
            cursor.execute(sql, airline)
            connector.commit()
        except pymysql.Error as n:
            print(n)

            # -------------- input params cannot locate a specific item--------------
        if cursor.rowcount == 0:
            print("Query error: input params cannot locate a specific item.")
            sys.exit(2)
        else:
            # -------------- Result format and output-------------
            columns = ["YEAR",
                       "AIRLINE_ID",
                       "Carrier code",
                       "Carrier name",
                       "ENTITY",
                       "GENERAL_MANAGE",
                       "PILOTS_COPILOTS",
                       "OTHER_FLT_PERS",
                       "PASS_GEN_SVC_ADMIN",
                       "MAINTENANCE",
                       "ARCFT_TRAF_HANDLING_GRP1",
                       "GEN_ARCFT_TRAF_HANDLING",
                       "AIRCRAFT_CONTROL",
                       "PASSENGER_HANDLING",
                       "CARGO_HANDLING",
                       "TRAINEES_INTRUCTOR",
                       "STATISTICAL",
                       "TRAFFIC_SOLICITERS",
                       "OTHER",
                       "TRANSPORT_RELATED",
                       "TOTAL"
                       ]
            self.query_print(columns, cursor)
            sys.exit(0)

    def do_aa(self, connector, carrier, origin):
        """
        air_carrier_summary_airport_activity table query operation
        """

        # -------------- execute SQL --------------
        cursor = connector.get_connection()
        sql = self.sql_statement["aa_query"]

        try:
            cursor.execute(sql, (carrier, origin))
            connector.commit()
        except pymysql.Error as n:
            print(n)

        # -------------- input params cannot locate a specific item--------------
        if cursor.rowcount == 0:
            print("Query error: input params cannot locate a specific item")
            sys.exit(2)
        else:
            # -------------- Result format and output-------------
            columns = ["OP_UNIQUE_CARRIER",
                       "CARRIER_NAME",
                       "ORIGIN_AIRPORT_ID",
                       "SERVICE_CLASS",
                       "REV_ACRFT_DEP_PERF_510",
                       "REV_PAX_ENP_110"
                       ]
            self.query_print(columns, cursor)
            sys.exit(0)
