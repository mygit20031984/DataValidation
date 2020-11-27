import pandas as pd
import numpy as np

def dtype_convert(df1, df2):
    datatype_dict_src = {}
    for col in df1.columns:
        _type = df1[col].dtype
        new_key = col
        new_val = _type
        datatype_dict_src[new_key] = new_val
    datatype_dict_tgt = {}
    col = 0
    for col in df2.columns:
        _type = df2[col].dtype
        new_key = col
        new_val = _type
        datatype_dict_tgt[new_key] = new_val
    col = 0
    k1 = set(datatype_dict_src.keys())
    k2 = set(datatype_dict_tgt.keys())
    common_keys = set(k1).intersection(set(k2))
    col = 0
    for col in common_keys:
        if datatype_dict_src[col] != datatype_dict_tgt[col]:
            if (datatype_dict_src[col] == 'object' and (
                    datatype_dict_tgt[col] == 'int64' or datatype_dict_tgt[col] == 'float64')):
                # src_df01[col] = src_df01[col].convert_objects(convert_numeric=True)
                # df1[col] = pd.to_numeric(df1[col])
                df1[col] = df1[col].apply(pd.to_numeric, errors='ignore')
            elif (datatype_dict_tgt[col] == 'object' and (
                    datatype_dict_src[col] == 'int64' or datatype_dict_src[col] == 'float64')):
                df2[col] = df2[col].apply(pd.to_numeric, errors='ignore')
            elif (datatype_dict_src[col] == 'int64' and datatype_dict_tgt[col] == 'float64'):
                df1[col] = df1[col].astype(np.float64)  # astype('float64')
                # df1[col = df1[col].div(100)  ##can be commnted if no needed
            elif (datatype_dict_src[col] == 'float64' and datatype_dict_tgt[col] == 'int64'):
                df1[col] = df1[col].astype(np.int64)
            ##need to add similar condition for datetime64


def find_dup1(df):
    dfObj = pd.DataFrame(df)
    # duplicateRows_fileName = parent_dir + "/" + TCN + '/Duplicates_in_' + df_name + "_" + run_id + '.csv'
    duplicateRowsDF = pd.DataFrame()
    duplicateRowsDF = dfObj[dfObj.duplicated(keep=False)]
    # duplicateRowsDF.to_csv(duplicateRows_fileName, index=False)
    return duplicateRowsDF