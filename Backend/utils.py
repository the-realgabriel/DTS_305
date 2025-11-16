import pandas as pd

def build_features(df):
    df["lag_1"] = df["value"].shift(1)
    df["lag_2"] = df["value"].shift(2)
    df["lag_3"] = df["value"].shift(3)
    df["rolling_3"] = df["value"].rolling(3).mean()
    df["rolling_5"] = df["value"].rolling(5).mean()
    df = df.dropna()
    return df
