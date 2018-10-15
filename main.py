#!/usr/bin/python
from __future__ import print_function

import re
import sys
import csv


def main():
    tableinfo = {}
    read_metadata(tableinfo)
    parse(sys.argv, tableinfo)


# Store metadata in a dict: tableinfo
def read_metadata(tableinfo):
    flag = 0
    f = open("metadata.txt", "r")
    for line in f:
        if line.strip() == "<begin_table>":
            flag = 1
        elif flag == 1:
            flag = 0
            table = line.strip()
            tableinfo[table] = []
        elif flag == 0 and not line.strip() == "<end_table>":
            info=line.strip()
            tableinfo[table].append(info)
        else:
            continue
    return


# Parsing the query
def parse(inputString, tableinfo):
    flag_distinct = 0
    flag_d = 0
    agg = 0
    inputString.pop(0)
    
    query = ' '.join(inputString)
    query = query.replace("FROM", "from")
    query = query.replace("From", "from")
    query = query.replace("Select", "select")
    query = query.replace("SELECT", "select")
    query = query.replace("WHERE", "where")
    query = query.replace("Where", "where")

    if "select" not in query or "from" not in query:
        sys.exit("Query syntax Error")

    partsWhere = query.split("where")
    partsWhere[0] = partsWhere[0][7:]
    partsFrom = partsWhere[0].split("from")

    query_tables = partsFrom[1].split(',')
    for i in query_tables:
        query_tables[query_tables.index(i)] = (re.sub(' +', ' ', i)).strip()
        i = i.strip()
        if i not in tableinfo:
            sys.exit("Table doesn't exist")
    columns = []
    columnsWithDots = []

    if partsFrom[0][0].strip() == '*':
        if len(query_tables) == 1:
            columns = tableinfo[query_tables[0]]
        if len(query_tables) > 1:
            columns = tableinfo[query_tables[0]] + tableinfo[query_tables[1]]
            columnsWithDots = [query_tables[0] + '.' + s for s in tableinfo[query_tables[0]]] + [query_tables[1] + '.' + s for s in tableinfo[query_tables[1]]]
        for i in columns:
            columns[columns.index(i)] = (re.sub(' +', ' ', i)).strip()

    elif "distinct" in partsFrom[0] and "distinct(" not in partsFrom[0] and "distinct (" not in partsFrom[0]:
        partsFrom[0] = partsFrom[0][9:]
        flag_distinct = 1
        columns = partsFrom[0].split(',')
        for i in columns:
            columns[columns.index(i)] = (re.sub(' +', ' ', i)).strip()

    elif "distinct(" in partsFrom[0] or "distinct (" in partsFrom[0]:
        if ',' in partsFrom[0]:
            sys.exit("Format of distinct wrong. Write without parentheses")
        columns = partsFrom[0]
        flag_d = 1
        columns = columns.strip()

    elif "max" in partsFrom[0] or "min" in partsFrom[0] or "avg" in partsFrom[0] or "sum" in partsFrom[0]:
        columns = partsFrom[0]
        agg = 1
        columns = columns.strip()
    else:
        try:
            columns = partsFrom[0].split(',')
            columns = [s.strip() for s in columns]

            if '.' in columns[0]:
                columnsWithDots = columns
            else:
                if len(query_tables) > 1:
                    check_column_ambiguity(columns, query_tables, tableinfo)
                for i in columns:
                    if i in tableinfo[query_tables[0]] :
                        columnsWithDots += [query_tables[0] + '.' + i]
                    elif len(query_tables) > 1:
                        if i in tableinfo[query_tables[1]]:
                            columnsWithDots += [query_tables[1] + '.' + i]
                        else:
                            # sys.exit('Incorrect or ambiguous column names')
                            raise Exception(i)
                    else:
                        raise Exception(i)
        except Exception as error:
            print("ERROR: Incorrect or ambiguous column name: ", end='')
            print(error.args)
            print("Exiting...")
            sys.exit(0)

    if len(partsWhere) > 1 and len(query_tables) == 1:
        partsWhere[1] = (re.sub(' +', ' ', partsWhere[1])).strip();
        func_where_withoutjoin(columns, query_tables, tableinfo, partsWhere[1])
        return

    if flag_distinct == 1:
        func_distinctquery(columns, query_tables, tableinfo)
        return

    if len(partsWhere) > 1 and len(query_tables) > 1:
        partsWhere[1] = (re.sub(' +', ' ', partsWhere[1])).strip();
        # func_where_join(columns, tables, tableinfo, parts[1])
        func_where_with_join(columnsWithDots, query_tables, tableinfo, partsWhere[1])
        return

    if len(query_tables) > 1:
        # func_join(columns, query_tables, tableinfo)
        func_join(columnsWithDots, query_tables, tableinfo)
        return

    if agg == 1:
        if '(' in columns and ')' not in columns:
            sys.exit("Syntax Error with aggregate query")
        else:
            yes = columns.split("(")

            agg_func = (re.sub(' +', ' ', yes[0])).strip()
            col = (re.sub(' +', ' ', yes[1].split(')')[0])).strip()
            if ',' in col:
                sys.exit("Wrong aggregate func")
            func_aggregate(agg_func, col, query_tables[0], tableinfo)
            return

    if flag_d == 1:
        func_dis(columns, query_tables, tableinfo)
        return

    func_select(columns, query_tables, tableinfo)


def check_column_ambiguity(columns, query_tables, tableinfo):
    for column in columns:
        if column in tableinfo[query_tables[0]] and column in tableinfo[query_tables[1]]:
            sys.exit("ambiguous column names: " + column)


def func_join(columns, tables, tableinfo):
    for i in columns:
        if columns.index(i) == len(columns)-1:
            print(i)
        else:
            print(i+",", end='')

    list1 = []
    list2 = []

    tables.reverse()

    t = tables[0] + '.csv'
    f = open(t, "r")
    linesFromCSV = csv.reader(f)
    for line in linesFromCSV:
        list1.append(line)
    t = tables[1] + '.csv'
    f = open(t, "r")
    linesFromCSV=csv.reader(f)
    for line in linesFromCSV:
        list2.append(line)

    file_data=[]

    for data2 in list2:
        for data1 in list1:
            file_data.append(data2 + data1)

    indexToPrint = []
    for column in columns:
        if '.' in column:
            tables_and_col = column.split('.')
            if tables_and_col[0] == tables[1]:
                var = tableinfo[tables[1]].index(tables_and_col[1])
                indexToPrint.append(var)
            elif tables_and_col[0] == tables[0]:
                var = len(tableinfo[tables[1]]) + tableinfo[tables[0]].index(tables_and_col[1])
                indexToPrint.append(var)

    # # print file_data
    for row in file_data:
        for counter in range(len(row)):
            if counter in indexToPrint:
                print(row[counter].strip()+"\t", end='')
        print()


def func_where_withoutjoin(columns, tables, tableinfo, where_cond):
    flag=0
    print_column_header(columns, tables, tableinfo)

    file_data = []
    part_where = where_cond.split(" ")
    t = tables[0] + '.csv'
    f = open(t, "r")
    linesFromCSV = csv.reader(f)
    for line in linesFromCSV:
        file_data.append(line)
    for val in file_data:
        s = ''
        for temp in part_where:
            if temp in tableinfo[tables[0]]:
                s += val[tableinfo[tables[0]].index(temp)]
            elif temp.lower() == 'or' or temp.lower() == 'and':
                s += " " + temp.lower() + " "
            elif temp == "=":
                s += "=="
            else:
                s += temp
        for column in columns:
            if eval(s):
                print(val[tableinfo[tables[0]].index(column)] + " ", end='')
                flag = 1
        if flag == 1:
            print()
            flag = 0


def func_dis(columns, tables, tableinfo):
    columns=columns[8:]
    columns=columns.strip()
    columns=columns.strip('(')
    columns=columns.strip(')')
    func_distinctquery(columns, tables, tableinfo)


def func_aggregate(agg_func, col, table, tableinfo):
    if col not in tableinfo[table] or col=='*':
        sys.exit("Syntax Error:Wrong column")
    t = table + '.csv'
    f = open(t, "r")
    linesFromCSV = csv.reader(f)
    cols = []
    for line in linesFromCSV:
        cols.append(int(line[tableinfo[table].index(col)]))

    if agg_func.lower() == "sum":
        print("sum(" + table + '.' + col + ")")
        print(sum(cols))
    elif agg_func.lower() == "max":
        print("max(" + table + '.' + col + ")")
        print(max(cols))
    elif agg_func.lower() == "min":
        print("min(" + table + '.' + col + ")")
        print(min(cols))
    elif agg_func.lower() == "avg":
        print("avg(" + table + '.' + col + ")")
        print(float(sum(cols))/len(cols))

    else:
        print ("Syntax Error")


def func_distinctquery(columns, tables, tableinfo):
    print_column_header(columns, tables, tableinfo)
    flag = 0
    copycheck = []
    for table in tables:
        # t = table + '.csv'
        # f=open(t,"r")
        t = table + '.csv'
        f = open(t, "r")
        linesFromCSV = csv.reader(f)

        for row in linesFromCSV:
            s = ""
            for colPos in range(len(columns)):
                s += row[tableinfo[tables[0]].index(columns[colPos])].strip()
                s += "\t"
            if s not in copycheck:
                copycheck.append(s)
                print(s, end='')
                flag = 1
            print()

            if flag == 1:
                flag = 0


def print_column_header(columns, tables, tableinfo):
    s = ''
    for column in columns:
        for table in tables:
            if column in tableinfo[table]:
                if s == '':
                    mark = 1
                else:
                    mark = 0
                if mark == 0:
                    s += ','
                s += table + '.' + column
    print (s)


def func_where_with_join(columns, tables, tableinfo, where_cond):
    part_where = where_cond.split(" ")
    for imp in columns:
        if columns.index(imp) == len(columns)-1:
            print(imp, end='')
        else:
            print (imp + ',', end='')
    print()

    listTable2 = []     # data of table 2
    listTable1 = []     # data of table 1

    tables.reverse()
    t = tables[0] + '.csv'
    f = open(t, "r")
    linesInCSV = csv.reader(f)

    for line in linesInCSV:
        listTable2.append(line)

    t = tables[1] + '.csv'
    f = open(t, "r")
    linesInCSV = csv.reader(f)

    for line in linesInCSV:
        listTable1.append(line)

    file_data = []

    for data1 in listTable2:
        for data2 in listTable1:
            file_data.append(data2+data1)

    tableinfo["join"] = []
    tableinfo["nodot"] = []

    for val in tableinfo[tables[1]]:
        tableinfo["join"].append(tables[1] + '.' + val)
    for val in tableinfo[tables[0]]:
        tableinfo["join"].append(tables[0] + '.' + val)
    tableinfo["nodot"] = tableinfo[tables[1]] + tableinfo[tables[0]]

    kk1 = tables[0]
    kk2 = tables[1]
    tables.remove(tables[0])
    tables.remove(tables[0])
    tables.insert(0, "join")  # may be removed after removing the usage of tables[0] below

    for val in file_data:
        s = ''
        for temp in part_where:
            if temp in tableinfo[kk1]:
                jj = temp
                temp = kk1+'.'+jj
            if temp in tableinfo[kk2]:
                jj = temp
                temp = kk2+'.'+jj
            if temp in tableinfo['join']:
                s += val[tableinfo["join"].index(temp)]
            elif temp.lower() == 'or' or temp.lower() == 'and':
                s += " " + temp.lower() + " "
            elif temp == "=":
                s += "=="
            else:
                s += temp
        if eval(s):
            for column in columns:
                if '.' in column:  # currently '.' will always be there in column (innerjoin)
                    print(val[tableinfo[tables[0]].index(column)].strip(), end='')
                    if columns.index(column) == len(columns)-1 :
                        print()
                    else:
                        print(',', end='')
                else:
                    print(val[tableinfo["nodot"].index(column)], end='')


def func_select(columns, tables, tableinfo):
    for term in columns:
        if term not in tableinfo[tables[0]]:
            sys.exit("Column not present")
    print_column_header(columns, tables, tableinfo)
    t = tables[0] + '.csv'
    file_data = []
    f = open(t, "r")
    linesFromCSV = csv.reader(f)
    for line in linesFromCSV:
        file_data.append(line)


    for line in file_data:
        for column in columns:
            print(line[tableinfo[tables[0]].index(column)] + " ", end='')
        print()


if __name__ == "__main__":
    main()