from datetime import datetime
import os
import pandas as pd
from code_scripts.parsers import read_csv, read_avro, read_json, read_delimited_file
from code_scripts import qasfunctions
from Results.Report_20201127_113117 import testcompare

start_dt = datetime.now()
#############################################
# Reading Input File
#############################################
xl = pd.ExcelFile("code_scripts/Input_file.xlsx")
dfx1 = xl.parse("Input")
dfx2 = xl.parse("Report_Location")
###########################################################
# Setting up Parent Folder and Run_ID
###########################################################
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
parent_dir = dfx2['Report_Location'][0] + "_" + run_id
os.makedirs(parent_dir)
print("Report directory for this run is Created")
tc_final_sts_listt = []
dirss = []
for index, row in dfx1.iterrows():
    TCN = row['Test_Case_Name']
    SFP = row['Source_File_Path']
    SFN = row['Source_File_Name']
    ST = row['Source_Type']
    SSQL = row['Source_SQL']
    TFP = row['Target_File_Path']
    TFN = row['Target_File_Name']
    UK = row['Unique_Keys']
    RF = row['Run_Flag']
    TT = row['Target_Type']
    TSQL = row['Target_SQL']
    SCONNME = row["src_connection_name"]
    TCONNME = row["tgt_connection_name"]
    SDELM = row["Src_Delimiter"]
    TDELM = row["Tgt_Delimiter"]
    IS_HEADER_SRC = row["Src_Is_Header_Present"]
    IS_HEADER_TGT = row["Tgt_Is_Header_Present"]
    if pd.isnull(row["Src_Header"]):
        SRC_HEADER = 0
    else:
        SRC_HEADER = row["Src_Header"].split(",")
    if pd.isnull(row["Tgt_Header"]):
        TGT_HEADER = 0
    else:
        TGT_HEADER = row["Tgt_Header"].split(",")
    #
    if str.upper(RF) == 'Y':
        if str.upper(ST) in {"CSV", "AVRO", 'JSON', "DELIMITED"}:
            f1 = SFP + '/' + SFN
        elif str.upper(ST) == 'ORACLE':
            f1 = SSQL
        if str.upper(TT) in {"CSV", "AVRO", 'JSON', "DELIMITED"}:
            f2 = TFP + '/' + TFN
        elif str.upper(TT) == 'ORACLE':
            f2 = TSQL
        print("====================================================")
        print("Please find below logs for Test Case Name = " + TCN)
        print("====================================================")
        # ####################################################
        # # Create Dataframes using Parsers#
        # ####################################################
        print("Creating Dataframes for Source and Target")
        if ST == 'CSV':
            df1 = read_csv.read_csv_df(f1)
        elif ST == 'AVRO':
            df1 = read_avro.read_avro_df(f1, encoding='utf-8')
        elif ST == 'JSON':
            df1 = read_json.read_json_df(f1)
        elif ST == 'DELIMITED':
            df1 = read_delimited_file.read_dlm_df(f1, SDELM, IS_HEADER_SRC, SRC_HEADER)
        elif ST == 'ORACLE':
            df1 = read_oracle.read_oracle_df(f1, SCONNME)
        #
        # creating df for target
        if TT == 'CSV':
            df2 = read_csv.read_csv_df(f2)
        elif TT == 'AVRO':
            df2 = read_avro.read_avro_df(f2, encoding='utf-8')
        elif TT == 'JSON':
            df2 = read_json.read_json_df(f2)
        elif ST == 'DELIMITED':
            df2 = read_delimited_file.read_dlm_df(f2, TDELM, IS_HEADER_TGT, TGT_HEADER)
        elif TT == 'ORACLE':
            df2 = read_oracle.read_oracle_df(f2, TCONNME)
        # creating subdirectory under parent dir
        if not os.path.exists(parent_dir + "/" + TCN):
            os.makedirs(parent_dir + "/" + TCN)
        count_of_src_rec = df1.shape[0]
        count_of_tgt_rec = df2.shape[0]
        # ####################################################
        # # Split Keys to List
        # ####################################################
        def clean_key(val):
            if val.isnumeric():
                return ''
            else:
                str_to_list = val.split(",")
                return str_to_list
        df1.columns = map(str.lower, df1.columns)
        df2.columns = map(str.lower, df2.columns)
        key = clean_key(str.lower(UK))
        # start_dt = datetime.now()
        qasfunctions.dtype_convert(df1[key], df2[key])
        qasfunctions.dtype_convert(df1, df2)
        # ###################################################
        # # Find Duplicate from Data Sets
        # ###################################################
        print("Finding Duplicate records in Data Sets")
        df1_key = df1[key]
        df2_key = df2[key]
        df1_dup = qasfunctions.find_dup1(df1_key).sort_values(key, inplace=False)
        df2_dup = qasfunctions.find_dup1(df2_key).sort_values(key, inplace=False)
        # print("Number of Duplicate Keys in DF1 for  " + TCN + " = " + str(df1_dup.shape[0]))
        # print("Number of Duplicate Keys in DF2 for  " + TCN + " = " + str(df2_dup.shape[0]))
        # #######################################################
        # # Remove Duplicate from Source and Target Data Sets#
        # #######################################################
        print("Removing Duplicate records in Data Sets")
        df1 = df1.sort_values(key, inplace=False)
        df1 = df1.drop_duplicates(subset=key, keep=False)
        df2 = df2.sort_values(key, inplace=False)
        df2 = df2.drop_duplicates(subset=key, keep=False)
        # # #####################################################
        # # #Compare Source and Target Data Sets
        # # #####################################################
        print("Executing Compare script to create Summary Report for each Testcase")
        qascompare = testcompare.TestCompare(
            df1,  # source df
            df2,  # target df
            join_columns=key,
            abs_tol=0,  # Optional, defaults to 0
            rel_tol=0,  # Optional, defaults to 0
            sample_count=500,
            df1_name='Source',
            df2_name='Target',
            report_name="Report_" + TCN + '_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx",
            indiv_report_dir=parent_dir + "/" + TCN,
            summary_rpt_dir=parent_dir,
            ignore_spaces=True,
            ignore_case=True,
        )
        print("check1")
        # TestcaseWise excel report creation
        tc_final_sts = qascompare.excel_create(df1_dup, df2_dup, count_of_src_rec, count_of_tgt_rec)
        qascompare.mismatches()
        # Summary excel report creation
        df_temp = dfx1.loc[dfx1['Run_Flag'] == 'Y']
        df_temp = df_temp[['Test_Case_Name']].reset_index(drop=True)
        tc_final_sts_listt.append(tc_final_sts)
        df_temp['STATUS'] = pd.DataFrame(tc_final_sts_listt)
        dirss.append(parent_dir + "//" + TCN)
        df_temp['HYPERLINKS'] = pd.DataFrame(dirss)
        qascompare.clean_up(df1, df2)  # Clean up source and Target dataframes
qascompare.final_sts_excel(df_temp)
end_dt = datetime.now()
duration = (end_dt - start_dt).total_seconds()
print("====================================================")
print("Script started at : - " + str(start_dt))
print("Script ended at : - " + str(end_dt))
print("Total time taken in seconds : " + str(duration))
print("Testcasewise Reports are available at " + parent_dir)
print("Script Execution Completed")
print("====================================================")