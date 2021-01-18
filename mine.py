import os
import sqlparse
import sys
import csv

tablename_colname_dict = {}
possible_aggr_funcs = ["max", "min", "cou", "ave", "sum"]


def apply_aggr_func(req_cols, aggr):
    ans = ""

    for i in range(len(req_cols)):
        dummy = []
        for j in range(len(all_data)):
            dummy.append(all_data[j][req_cols[i]])
        if aggr[i].lower() == "min":
            try:
                min_val = min(dummy)
            except ValueError:
                print ("value error : min value not found")
                sys.exit()
            ans += str(min_val) + "\t"

        elif aggr[i].lower() == "max":
            try:
                max_val = max(dummy)
            except ValueError:
                print ("value error : max value not found")
                sys.exit()
            ans += str(max_val) + "\t"

        elif aggr[i].lower() == "count":
            try:
                count = len(dummy)
            except ValueError:
                print ("value error : counting not possible on this \n")
                sys.exit()
            ans += str(count) + "\t"

        elif aggr[i].lower() == "average":
            try:
                avg = round(sum(dummy)/len(dummy),2)
            except ValueError:
                print ("value error : avg value not found")
                sys.exit()
            ans += str(avg) + "\t"

        elif aggr[i].lower() == "sum":
            try:
                sum_val = sum(dummy)
            except ValueError:
                print ("value error : sum not possible")
                sys.exit()
            ans += str(sum_val) + "\t"
        else:
            print("function not recognized\n")
            sys.exit()
    return ans


def remove_duplicate(data):
    data = data.split('\n')
    output = []
    for row in data:
        if row not in output:
            output.append(row)
    return '\n'.join(output)


def remove_quotes(line):
    for i in range(len(line)):
        if (line[i][0] == "\'" or line[i][0] == '\"') and (line[i][0] == line[i][-1]):
            line[i] = line[i][1:-1]

    return line


def read_csv(filename, is_distinct_present):
    filename = filename+".csv"
    print("filename : ", filename)
    table_data = []
    try:
        csvfile = csv.reader(open(filename), delimiter=',')
    except Exception:
        print("error in csv file reading, check if file exists\n")
        sys.exit()

    for row in csvfile:
        row = remove_quotes(row)
        row = list(map(int, row))

        if is_distinct_present == False or (is_distinct_present and row not in table_data):
            table_data.append(row)

    return table_data


def getJoinedData(table_names_list, is_distinct_present):
    table_data = read_csv(table_names_list[0], is_distinct_present)
    if len(table_names_list) == 1:
        return table_data
    else:
        for tablename in table_names_list[1:]:
            table2 = read_csv(tablename, is_distinct_present)
            dummy = []
            for i in table_data:
                for j in table2:
                    dummy.append(i+j)
            table_data = dummy
        return table_data


def seperate_aggr(query_cols_with_aggr):
    aggr_present = False
    normal_col = False
    aggr = []
    query_cols = []
    for col in query_cols_with_aggr:
        col = col.strip()
        if col.lower()[:3] in possible_aggr_funcs:
            aggr.append(col.split("(")[0])
            query_cols.append(col.split("(")[1][:-1])
            aggr_present = True
        else:
            query_cols.append(col)
            normal_col = True
        if normal_col and aggr_present:
            print(
                "normal columns and aggr function with column are present simultaneuosly")
            sys.exit()
    return query_cols, aggr


def readQuery(query):
    query = query[:-1]
    print("query : ", query)
    parsed_query = sqlparse.parse(query)[0].tokens

    is_select = sqlparse.sql.Statement(parsed_query).get_type()

    all_idenetifiers = sqlparse.sql.IdentifierList(
        parsed_query).get_identifiers()
    all_idenetifiers = list(map(str, all_idenetifiers))
    print("all_identifiers : ", all_idenetifiers)
    is_distinct_present = False
    is_where_present = False
    from_flag = False
    condition = ""
    table_names = ""
    table_names_list = []
    is_groupby_present=False
    is_orderby_present=False
    groupby_flag=False
    groupby_col=""
    orderby_flag=False
    orderby_col=""

    for identifier in all_idenetifiers:
        if identifier.lower() == 'distinct':
            is_distinct_present = True
        elif identifier.lower() == 'from':
            from_flag = True
        elif identifier.lower()[0:5] == 'where':
            is_where_present = True
            condition = identifier[6:].strip()
        elif from_flag:
            table_names = identifier
            table_names_list = table_names.split(",")
            table_names_list = list(map(str.strip, table_names_list))
            from_flag = False
        elif identifier.lower()[0:5]=='groupby':
            groupby_flag=True
            is_groupby_present=True
        elif groupby_flag:
            groupby_col = identifier
            groupby_flag=False
        elif identifier.lower()[0:5]=='orderby':
            orderby_flag=True
            is_orderby_present = True
        elif orderby_flag:
            orderby_col = identifier
            orderby_flag=False
    """
    if(dist>1):
        print ("ERROR : syntax error with the usage of distinct")
        sys.exit()

    if where ==1 and len(condition.strip())==0:
        print ("ERROR : syntax error in where clause")
        sys.exit()

    if len(components)> 5 and where ==0 :
        print ("ERROR : syntax error")
        sys.exit()

    if len(components)== 5 and where ==0 and dist==0:
        print (" ERROR : syntax error")
        sys.exit()
    """

    if is_distinct_present:
        query_cols_with_aggr = all_idenetifiers[2]
    else:
        query_cols_with_aggr = all_idenetifiers[1]

    query_cols_with_aggr = query_cols_with_aggr.split(",")
    query_cols, aggrs = seperate_aggr(query_cols_with_aggr)
    print("query cols : ", query_cols)
    print("aggrs : ", aggrs)
    print("table name lst : ", table_names_list)

    if is_select.lower() != 'select':
        print("INVALID QUERY : No SELECT statement\n")
        sys.exit()

    # all_columns_in_sequence = find_queried_columns(table_names)
    global all_data
    all_data = getJoinedData(table_names_list, is_distinct_present)
    print("all data : ", len(all_data))
    headers = ""
    all_col_names_list = []
    # for value in tablename_colname_dict.values():
    #     all_col_names_list = all_col_names_list + value

    for table_name in table_names_list:
        all_col_names_list = all_col_names_list + \
            tablename_colname_dict[table_name]

    print("all col : ", all_col_names_list)

    for c in query_cols:
        headers = headers + c + "\t"
    headers += "\n"

    req_cols_index = []
    if len(query_cols) == 1 and query_cols[0] == '*':
        headers=""
        for col in all_col_names_list:
            headers = headers + col + "\t"
        headers += "\n"
        for i in range(0, len(all_col_names_list)):
            req_cols_index.append(i)
    else:
        for col in query_cols:
            print ("col in trial : ", col)
            try:
                req_cols_index.append(all_col_names_list.index(col))
                
            except Exception:
                print ("required col not found \n")
                sys.exit()
        
    print ("req cols index : ", req_cols_index)

    if len(aggrs)==0:
        # handle col as [*, A]  // np
        output = ""
        count=1
        for i in range(len(all_data)):
            for j in req_cols_index:
                output += str(all_data[i][j])+"\t"
                
            output+="\n"
        if is_distinct_present:
            output = remove_duplicate(output)
    else:
        output = apply_aggr_func(req_cols_index, aggrs)
    print("finaal outputwer : \n")
    print(headers+output)    
    print ("len of outputwer : ", len(all_data))

def read_metadata(filename):
    flag=0
    with open(filename, 'r') as f:
        for line in f:
            if line.strip() == '<begin_table>':
                columnnames = list()
                flag=1
            elif flag==1:
                tablename=line.strip()
                flag=0
            elif line.strip() == '<end_table>':
                tablename_colname_dict[tablename] = columnnames
            else:
                columnnames.append(line.strip())



def main():
    metadata_filename = "metadata.txt"
    read_metadata(metadata_filename)
    # print ("tablename_colname_dict : ", tablename_colname_dict)\
    sqlQuery = sys.argv[1]
    
    if sqlQuery[-1]!=';':
        print ("semicolon missing \n")
        sys.exit()

    readQuery(sqlQuery)

    # all_data = read_csv()



main()
