import os
import pandas as pd
from collections import Counter

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

def run_filter_primary():
    #columns names in csv file
    col = ["TMAX", 'TMIN', 'TAXN', 'PRCP', 'SNOW', 'SNWD', 'DATE', 'STATION']

    #set the current dir to where the script is located
    os.chdir(os.path.abspath( os.path.dirname( __file__ ) ))

    input_loc = r'Example Data\\'
    output_loc = r'Filtered Example Data'

    #filter file in dir to perfect & other folder based contents
    for item in os.listdir(input_loc):
        df = filter_csv(input_loc+ item, col)
        
        filter_df = check_empty_columns(df)
        key = filter_columns(filter_df)

        # check the key to see which folder to put csv file in Filtered Data
        if key == 'pt': filter_df.to_csv(os.path.join(output_loc, 'Perfect', item), index=False)
        if key == 'tp': filter_df.to_csv(os.path.join(output_loc, 'Temperature', item), index=False)
        if key == 'pr': filter_df.to_csv(os.path.join(output_loc, 'Precipitation', item), index=False)

def north_southHS():
    #set the current dir to where the script is located
    os.chdir(os.path.abspath( os.path.dirname( __file__ ) ))
    
    nth_loc = r'Other/northern-hemisphere-countries-2024.csv'
    sth_loc = r'Other/southern-hemisphere-countries-2024.csv'
    
    #hemisphere dataframes
    nth_df = pd.read_csv(nth_loc)
    sth_df = pd.read_csv(sth_loc)
    
    nth_hs = open(r'Other/northern_hemisphere.txt', 'w')
    sth_hs = open(r'Other/southern_hemisphere.txt', 'w')
    whole = open(r'Other/both_hemisphere.txt', 'w')
    
    locProp_nrow = 'NorthernHemisphereCountriesProportionInNorthernHemisphere'
    locProp_srow = 'InSouthernHemisphere'
    
    #append countries to each respective hemisphere file
    for index, row in nth_df.iterrows():
        if row[locProp_nrow] == 'Fully': nth_hs.write(row['country'] + '\n')
        elif row[locProp_nrow] == ' Partly' or row[locProp_nrow] == 'Mostly':
            whole.write(row['country'] + '\n')
    
    for index, row in sth_df.iterrows():
        if row[locProp_srow] == 'Entirely': sth_hs.write(row['country'] + '\n')
        elif row[locProp_srow] == 'Partially' or row[locProp_srow] == 'Mostly':
            whole.write(row['country'] + '\n')
           
    #closes file 
    nth_hs.close()
    sth_hs.close()
    whole.close()
    
def filter_threshold():
    #set the current dir to where the script is located
    os.chdir(os.path.abspath( os.path.dirname( __file__ ) ))
    
    data_loc = r'Filtered Example Data\\Perfect'
    output_loc = r'Filtered Example Data\\Perfect_thres'
    thershold_datapath = r'Other\\Table_Extreme_Records_Hemisphere.csv'
    
    #countries in each hemisphere
    with open(r'Other\\both_hemipshere.txt') as f:
        whole = f.readlines()
    with open(r'Other\\northern_hemisphere.txt') as f:
        nth_hs = f.readlines()
    with open(r'Other\\southern_hemisphere.txt') as f:
        sth_hs = f.readlines()
    
    #country code dic
    countrycode_dic = code_keydic()
    
    #threshold dataframe
    thres_df = pd.read_csv(thershold_datapath)
    tempMx, tempMn, prcp = [], [], []
    
    for index, row in thres_df.iterrows():
        if row['Characteristic'] == 'TMAX':
            if row['Hemisphere'] == 'Northern': tempMx.append(row['Value'])
            elif row['Hemisphere'] == 'Southern': tempMx.append(row['Value'])
        elif row['Characteristic'] == 'TMIN':
            if row['Hemisphere'] == 'Northern': tempMn.append(row['Value'])
            elif row['Hemisphere'] == 'Southern': tempMn.append(row['Value'])            
        elif row['Characteristic'] == 'PRCP':
            if row['Hemisphere'] == 'Northern': prcp.append(row['Value'])
            elif row['Hemisphere'] == 'Southern': prcp.append(row['Value'])
    
    for item in os.listdir(data_loc):
        code = item[:2]
        country_name = code_to_country(code)
        df = pd.read_csv(item)
        
        if country_name in whole:
            if df['TMAX'].max() > max(tempMx): continue
            if df['TMIN'].min() < min(tempMn): continue
            if df['PRCP'].max() > max(prcp): continue
            
            #move files after data is validated
            orgi_loc = data_loc + item
            new_loc = output_loc + item
            os.replace(orgi_loc, new_loc)

        elif country_name in nth_hs:
            if df['TMAX'].max() > tempMx[0]: continue
            if df['TMIN'].min() < tempMn[0]: continue
            if df['PRCP'].max() > prcp[0]: continue

            #move files after data is validated
            orgi_loc = data_loc + item
            new_loc = output_loc + item
            os.replace(orgi_loc, new_loc)

        elif country_name in sth_hs:
            if df['TMAX'].max() > tempMx[1]: continue
            if df['TMIN'].min() < tempMn[1]: continue
            if df['PRCP'].max() > prcp[1]: continue

            #move files after data is validated
            orgi_loc = data_loc + item
            new_loc = output_loc + item
            os.replace(orgi_loc, new_loc)
    
def get_time_range():
    #set the current dir to where the script is located
    os.chdir(os.path.abspath( os.path.dirname( __file__ ) ))

    input_loc_pt = r'Filtered Example Data\\Perfect\\'
    input_loc_tp = r'Filtered Example Data\\Precipitation\\'
    input_loc_pr = r'Filtered Example Data\\Temperature\\'
    list_loc = [input_loc_pr, input_loc_pt, input_loc_tp]

    #initialize data structure to keep track of frequency
    start_ct = Counter({})
    end_ct = Counter({})

    #get info on each .csv file
    for i in list_loc:
        for item in os.listdir(i):
            item_path = i + item
            df = pd.read_csv(item_path)

            start_date = df['DATE'].min()
            end_date = df['DATE'].max()

            start_ct.update([start_date])
            end_ct.update([end_date])
    
    # get the most frequent start date
    mtComStart_ct = start_ct.most_common(1)[0][1]
    mtComStart_it = [item for item, count in start_ct.items() if count == mtComStart_ct]

    # get the most frequent end date
    mtComEnd_ct = end_ct.most_common(1)[0][1]
    mtComEnd_it = [item for item, count in end_ct.items() if count == mtComEnd_ct]

    # print the most frequent start and end date
    print("Most frequent Start Date - count:", mtComStart_ct, " item:", mtComStart_it)
    print("Most frequent End Date - count:", mtComEnd_ct, " item:", mtComEnd_it)

def code_keydic():
    #set the current dir to where the script is located
    os.chdir(os.path.abspath( os.path.dirname( __file__ ) ))
    
    code_loc = r'Other\\ghcnd-countries.txt'
    code_key = {}
        
    with open(code_loc) as f:
        data = f.readlines()
        
    for line in data:
        code, country = line.split(' ', 1)
        code_key[code] = country

    return code_key

def code_to_country(txt, key):
    code = txt[:2]
    
    return key[code]
    
if __name__ == "__main__":
    #run first filter function
    #run_filter_primary()

    #get range of time
    #get_time_range()
    #north_southHS()
    filter_threshold()