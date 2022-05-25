import sys
import re
import getopt

city_regex = '^[A-Z][a-z]*$'
month_regex = '^(0?[1-9]|1[012])$'
date_regex = '^[0-9]{2}$'
dw_regex = '^[1-7]$'
flightNum_regex = '^[0-9]{1,4}$'


class Helper(object):

    def __init__(self):
        # ---------- dict query_params initialization----------
        self.params = {"oc": "NULL", "dc": "NULL", "flight_num": "NULL", "month": "NULL",
                       "date": "NULL",
                       "dw": "NULL"}

    def get_params(self):
        return self.params

    def param_parse(self, options_parse):
        # --------- Process the parsed result ----------
        for option, value in options_parse:
            if option in ("-q", "--query", "-m", "--modify", "-d", "--delete", "-i", "--insert", "-h", "--help"):
                continue

            # --------- Process the argument [--oc] ----------
            elif option in '--oc':
                if re.match(city_regex, value):
                    self.params["oc"] = value
                else:
                    print("origin city [--oc val] format error")
                    sys.exit(2)

            # --------- Process the argument [--dc] ----------
            elif option in '--dc':
                if re.match(city_regex, value):
                    self.params["dc"] = value
                else:
                    print("destination city [--dc val] format error")
                    sys.exit(2)

            # --------- Process the argument [--month] ----------
            elif option in '--month':
                if re.match(month_regex, value):
                    self.params["month"] = value
                else:
                    print("month [--month val]format error")
                    sys.exit(2)

            # --------- Process the argument [--date] ----------
            elif option in '--date':

                def date_check(month, date):
                    """

                    Parameters
                    ----------
                    month: input argument month
                    date: input argument date

                    Returns: True for valid date, False for invalid date
                    -------

                    """
                    month_31 = ["01", "03", "05", "07", "08", "1", "3", "5", "7", "8", "10", "12"]
                    month_30 = ["04", "06", "09", "4", "6", "9", "11"]
                    month_29 = ["02", "2"]

                    if (month in month_31 and date <= "31") or (month in month_30 and date <= "30") or (
                            month in month_29 and date <= "29"):
                        return True
                    else:
                        return False

                # No month only date: is invalid
                if self.params["month"] == "NULL":
                    print("month date order error")
                    sys.exit(2)
                else:
                    # if date is valid: put it into the dictionary
                    if re.match(date_regex, value) and date_check(self.params["month"], value):
                        self.params["date"] = value
                    else:
                        # if date is invalid: raise an error and exit
                        print("date [--date val] format error")
                        sys.exit(2)

            # --------- Process the argument [--dw] ----------
            elif option in '--dw':
                if re.match(dw_regex, value):
                    self.params["dw"] = value
                else:
                    print("day of week [--dw val] format error")
                    sys.exit(2)

            # --------- Process the argument [--fn] ----------
            elif option in '--fn':
                if re.match(flightNum_regex, value):
                    self.params["flight_num"] = value
                else:
                    print("flight number [--fn val] format error")
                    sys.exit(2)

            # --------- invalid arguments ----------
            else:
                print("Query error: invalid arguments.")

        # --------- Parsed result validation check --------------------------------
        # either month + date or month + dateOfWeek
        if self.params["month"] != "NULL" and self.params["date"] != "NULL" and self.params["dw"] != "NULL":
            print("month/day_of_week format error")
            sys.exit(2)

    def parse_helper(self, args):
        """
        Parse the input arguments to a dictionary and return
        """

        # ---------- Parse command line options and parameter list ----------
        try:
            options, argv = getopt.getopt(args, "qmd",
                                          ["query", "modify", "delete", "oc=", "dc=", "fn=",
                                           "month=",
                                           "date=",
                                           "dw=",
                                           ])
        except getopt.GetoptError:
            print("Query error: invalid arguments.")
            sys.exit(2)

        self.param_parse(options)

    @staticmethod
    def insert_helper():
        insert_param = []
        output = ["Month: ",
                  "Day of the month: ",
                  "Day of the week: ",
                  "Carrier code: ",
                  "Unique tail number: ",
                  "Flight number: ",
                  "Origin airport ID: ",
                  "Origin airport abbreviation: ",
                  "Origin city name: ",
                  "Destination airport ID: ",
                  "Destination airport abbreviation: ",
                  "Destination city name: ",
                  "Planned departure time: ",
                  "Actual departure time: ",
                  "Departure delay in minutes: ",
                  "TARGET VARIABLE Binary if delayed over 15 min, 1 is yes: ",
                  "Departure time block: ",
                  "Planned arrival time: ",
                  "Actual arrival time: ",
                  "Arrival delay in minutes: ",
                  "Arrival time block: ",
                  "Flag if flight was cancelled: ",
                  "Cancellation Code: ",
                  "Flight planned elapsed time: ",
                  "Flight actual elapsed time: ",
                  "Flight Distance in miles: ",
                  "Flight distance group: ",
                  "Flag for a carrier delay: ",
                  "Flag for a weather delay: ",
                  "Flag for a NAS delay: ",
                  "Flag for a security delay: ",
                  "Flag for a late aircraft delay: "]

        for reminder in output:
            insert_param.append(input(reminder))

        return insert_param

    @staticmethod
    def help_helper():
        print("Help Information:")
        # ---------- Query delay help Information----------
        print("1. Query the delay data    python3 ece656_main.py [-q/--query] [args1 val1] ...")
        print("\t--arguments definitions:")
        print(
            "\t1.1 [--oc val] + [--month val]/[--date val]/[--date_week val]:")
        print("\tQuery the delay data of the flights from the origin city (with or without the time data)\n")
        print(
            "\t1.2 [--dc val] + [--month val]/[--date val]/[--date_week val]:")
        print("\tQuery the delay data of the flights to the destination city (with or without the time data)\n")
        print(
            "\t1.3 [--oc val][--dc val] + [--month val]/[--date val]/[--date_week val]:")
        print("\tQuery the delay data of the flights from the origin city to the destination city "
              "(with or without the time data)\n")
        print(
            "\t1.4 [--fn val] + [--month val]/[--date val]/[--date_week val]:")
        print("\tQuery the delay data of a specific flight (with or without the time data)\n")

        # ---------- Modify delay help Information----------
        print("2. Modify a delay data    python3 ece656_main.py [-m/--modify] [args1 val1] ...")
        print("\t--arguments definitions:")
        print("\t[--month val] [--date val] [--oc val] [--dc] [--fn val]:")
        print("\tModify the delay data of a specific flight with arguments of time data and flight data)\n")

        # ---------- Insert delay help Information----------
        print("3. Insert a delay data    python3 ece656_main.py [-i/--insert]\n")

        # ---------- Delete delay help Information----------
        print("4. Delete a delay data    python3 ece656_main.py [-d/--delete] [args1 val1] ...")
        print("\t--arguments definitions:")
        print("\t[--month val] [--date val] [--oc val] [--dc] [--fn val]:")
        print("\tDelete the delay data of a specific flight with arguments of time data and flight data)\n")

        # ---------- Query employee help Information----------
        print("5. Query employeement information    python3 ece656_main.py [-e]\n")

        # ---------- Query airline activity information help Information----------
        print("6. Query airline activity information    python3 ece656_main.py [-a]\n")
