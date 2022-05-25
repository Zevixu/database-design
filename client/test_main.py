import re
import csv
import sys

r1 = '[0-9]* flight'
r2 = '[0-9]*.[0-9]* minutes'
r3 = '[0-9]*'
r4 = '[0-9]*.[0-9]*'
pattern_1 = re.compile(r1)
pattern_2 = re.compile(r2)
pattern_3 = re.compile(r3)
pattern_4 = re.compile(r4)


def result_match():
    client_result = []
    sql_result = []
    with open("client_result.txt", "r") as c:
        for line in c.readlines():
            line = line.strip('\n')
            result_1 = pattern_1.findall(line)
            result_2 = pattern_2.findall(line)
            if len(result_1) != 0:
                tmp = pattern_3.findall(result_1[0])
                client_result.append(int(tmp[0]))
            if len(result_2) != 0:
                for item in result_2:
                    tmp = pattern_4.findall(item)
                    client_result.append(float(tmp[0]))

    with open("sql_result.csv", "r", newline='', encoding="UTF-8") as s:
        load = csv.reader(s, delimiter=',')
        for row in load:
            for item in row:
                if item != '\\N':
                    sql_result.append(item)

    if len(client_result) != len(sql_result):
        print("Failed")
    else:
        for i in range(0,len(client_result)):
            client = format(client_result[i], '.1f')
            sql = format(float(sql_result[i]), '.1f')
            if client != sql:
                print("Failed")
                sys.exit(1)

    print("Passed")


if __name__ == '__main__':
    result_match()
