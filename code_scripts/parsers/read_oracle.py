import cx_Oracle
import pandas as pd


def read_oracle_df(sql, connection_nme):
    orcl_file = "/code_scripts/parsers/oracle_connections.dat"
    fileHandle = open(orcl_file, 'r')
    for line in fileHandle:
        if connection_nme in line:
            fields = line.split('|')
            hostname = fields[1]  # prints the second fields value
            port = fields[2]
            sid = fields[3]
            user = fields[4]
            password = fields[5]
            list = hostname.split("=")
            Host_Name = list[1]
            list2 = port.split("=")
            port = (list2[1])
            list3 = user.split("=")
            userr = (list3[1])
            list4 = password.split("=")
            passw = (list4[1]).replace("\n", "")
            list5 = sid.split("=")
            sid = (list5[1])

            # if 'sid' in line:
            dsn_tns = cx_Oracle.makedsn(Host_Name, port, sid)
            print(dsn_tns)
            # if 'service_name' in line:
            #     dsn_tns = cx_Oracle.makedsn(Host_Name, port, service_name)
            conn = cx_Oracle.connect(userr, passw, dsn_tns, encoding="UTF-8")
            try:
                df = pd.read_sql(con=conn, sql=sql)
            finally:
                conn.close()
                print(df)
                return df
    fileHandle.close()
# read_oracle_df("select * from dual",'oracle2')
# orcl_file = "C:/Users/497523/DataComp/code_scripts/parsers/oracle_connections.dat"
# fileHandle = open(orcl_file, 'r')
# matched_lines = [line for line in fileHandle.readlines() if "oracle4" in line]
# print(matched_lines)
# matched_lines.split("|")[0]
# [i.split('|', 1)[1] for i in matched_lines]
# Type = matched_lines.split("|")
# x = Type[1]
# y = Type[2]
# for points in Type:
#     print(x,y)


