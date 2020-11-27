import pandas as pd

def read_json_df(f):
    df = pd.read_json(f)
    print(df)