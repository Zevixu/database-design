#!/opt/homebrew/opt/python@3.10/bin/python3
import helper
import sys
import getopt
import mysql_connector as sqlc
import mysql_processor as sqlp


def main(args):
    # ------------ initialization ------------
    mysql_connector = sqlc.MysqlConnector()
    mysql_processor = sqlp.Processor()
    handler = helper.Helper()

    # ------------ NO arguments: print the help message ------------
    if len(args) == 0:
        handler.help_helper()

    # ------------ parse arguments ------------------------
    # ------------ print error message and exit if argument is not valid------------
    try:
        options, argv = getopt.getopt(args, "qimdhea",
                                      ["query", "insert", "modify", "delete", "help", "employee",
                                       "oc=", "dc=", "fn=", "month=", "date=", "dw="])
    except getopt.GetoptError:
        print(f"input argument {args} error: use -h to check help information".format(args=args))
        sys.exit(2)

    # ------------ case switch ------------------------
    for option, value in options:

        # -----------  [-q/--query]: query the delay data --------
        if option in ("-q", '--query'):
            handler.parse_helper(args)
            query_params = handler.get_params()
            mysql_processor.do_query(mysql_connector, query_params)

        # -----------  [-i/--insert]: insert delay data --------
        elif option in ("-i", '--insert'):
            insert_params = helper.Helper.insert_helper()
            mysql_processor.do_insert(mysql_connector, insert_params)

        # -----------  [-m/--modify]: modify delay data --------
        elif option in ("-m", '--modify'):
            handler.parse_helper(args)
            modify_params = handler.get_params()
            mysql_processor.do_modify(mysql_connector, modify_params)

        # -----------  [-d/--delete]: delete delay data --------
        elif option in ("-d", '--delete'):
            handler.parse_helper(args)
            delete_params = handler.get_params()
            mysql_processor.do_delete(mysql_connector, delete_params)

        # -----------  [-e]: query employment data --------
        elif option in "-e":
            airline = input("Please enter the airline ID: ")
            mysql_processor.do_employee(mysql_connector, airline)

        # -----------  [-a]: query airport activity data --------
        elif option in "-a":
            carrier = input("Please enter the carrier code: ")
            origin = input("Please enter the origin airport ID: ")
            mysql_processor.do_aa(mysql_connector, carrier, origin)

        # -----------  [-h]: print help information --------
        elif option in ("-h", '--help'):
            handler.help_helper()


if __name__ == "__main__":
    main(sys.argv[1:])
