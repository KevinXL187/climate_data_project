import os
import pandas as pd

def filter_csv(input_file, columns):
    df_in = pd.read_csv(input_file, dtype=str)
    df_columns = df_in.columns.to_list()

    valid_col = [col for col in columns if col in df_columns]
    # missing_col = [col for col in columns if col not in df_columns]

    #Filter the DataFrame based on columns
    filtered_df = df_in[valid_col]

    return filtered_df

def check_empty_columns(df):
    #Filters out the empty columns
    filtered_df = df.dropna(axis=1, how="all")

    return filtered_df

def filter_columns(df):
    temp_col = ["TMAX", "TMIN"]
    avgTemp_col = ["TAXN"]
    other_col = ["PRCP", 'SNOW', 'SNWD']

    df_col = df.columns.to_list()

    temp_cond = all(col in df_col for col in temp_col)
    avgTemp_cond = all(col in df_col for col in avgTemp_col)

    if temp_cond or avgTemp_cond:
        if all(col in df_col for col in other_col): return 'pt' #add to perfect data
        else: return 'tp' #add to temp data
    else: return 'pr' #only precipitation data


if __name__ == "__main__":
    #columns names in csv file
    col = ["TMAX", 'TMIN', 'TAXN', 'PRCP', 'SNOW', 'SNWD', 'DATE', 'STATION']

    #set the current dir to where the script is located
    os.chdir(os.path.abspath( os.path.dirname( __file__ ) ))

    input_loc = r'Example Data\\'
    output_loc = r'Filtered Data'

    #filter file in dir to perfect & other folder based contents
    for item in os.listdir(input_loc):
        df = filter_csv(input_loc+ item, col)
        
        filter_df = check_empty_columns(df)
        key = filter_columns(filter_df)

        # check the key to see which folder to put csv file in Filtered Data
        if key == 'pt': filter_df.to_csv(os.path.join(output_loc, 'Perfect', item), index=False)
        if key == 'tp': filter_df.to_csv(os.path.join(output_loc, 'Temperature', item), index=False)
        if key == 'pr': filter_df.to_csv(os.path.join(output_loc, 'Precipitation', item), index=False)


