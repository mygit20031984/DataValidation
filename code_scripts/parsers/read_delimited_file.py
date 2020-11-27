import pandas as pd

def read_dlm_df(f, delimiter, is_header_present, header):
    if is_header_present == 'Y':
        df = pd.read_csv(f, sep=delimiter, engine='python')
    elif is_header_present == 'N':
        df = pd.read_csv(f, sep=delimiter, names=header, engine='python')
    return df