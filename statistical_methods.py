import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ipywidgets as widgets

from filter import code_keydic
from IPython.display import display

import os

# Data Aggregation
    # Temporal
    # Spatial

def find_first_last_occurrence(lst):
    occurrences = {}
    current, first = None, None

    for idx, item in enumerate(lst):
        if item[:2] != current:
            if current is not None: occurrences[current] = (first, idx)
            current = item[:2]
            first = idx
    
    if current is not None: occurrences[current] = (first, len(lst) - 1)

    return occurrences

def monthy_yearly_data():
    data_loc = r'Filtered Data\\Ready Data'
    output_loc =  r'Filtered Data\\Data Aggregation'
    
    lst_of_csv = os.listdir(data_loc)
    lst_of_csv.sort()
    country_code_dic = find_first_last_occurrence(lst_of_csv)

    for ccode in country_code_dic.keys():
        startIdx, endIdx = country_code_dic[ccode][0], country_code_dic[ccode][1]
        monthly_lst, yearly_lst = [], []

        for item in lst_of_csv[startIdx:endIdx]:
            df = pd.read_csv(os.path.join(data_loc, item))

            #create new year and month column from data column
            df['DATE'] = pd.to_datetime(df['DATE'])
            df['Year'] = df['DATE'].dt.year
            df['Month'] = df['DATE'].dt.month

            #calculate monthly and yearly average for each column
            col_to_include = df.columns[df.columns != 'STATION']
            monthly_avg = df.groupby(['Year', 'Month'])[col_to_include].mean()
            yearly_avg = df.groupby(['Year'])[col_to_include].mean()

            #append the results to the lists
            monthly_lst.append(monthly_avg)
            yearly_lst.append(yearly_avg)
        
        #concatenate all dataframe from all csv files that are in list
        country_monthly_avg = pd.concat(monthly_lst)
        country_yearly_avg = pd.concat(yearly_lst)

        #drop last two duplicate columns
        country_monthly_avg = country_monthly_avg.iloc[:, :-3]
        country_yearly_avg = country_yearly_avg.iloc[:, :-3]

        #
        country_monthly_avg  = country_monthly_avg.groupby(['Year', 'Month']).mean()
        country_yearly_avg = country_yearly_avg.groupby(['Year']).mean()

        # divide value in dataframe by 10 to get accurate units
        columns_to_divide_10 = ['TMAX', 'TMIN', 'PRCP']
        
        country_monthly_avg[columns_to_divide_10] = country_monthly_avg[columns_to_divide_10]/10
        country_yearly_avg[columns_to_divide_10] = country_yearly_avg[columns_to_divide_10]/10

        # round all the data besides timestamp
        columns_to_round = ['TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD']

        country_monthly_avg[columns_to_round] = country_monthly_avg[columns_to_round].round(4)
        country_yearly_avg[columns_to_round] = country_yearly_avg[columns_to_round].round(4)

        #save to csv file
        country_monthly_avg.to_csv(os.path.join(output_loc, f'monthly_avg_{ccode}.csv'))
        country_yearly_avg.to_csv(os.path.join(output_loc, f'yearly_avg_{ccode}.csv'))

def plot_year(dfs_by_year, year, country_name):
        df = dfs_by_year[year]

        #merge dfs to create df with all 12 months, even if data is missing for those months
        month_df = pd.DataFrame({'Month': range(1, 13)})
        df = pd.merge(month_df, df, on='Month', how='left')

        plt.figure(figsize=(8, 6))
        
        plt.bar(df['Month'], df['TMAX'].fillna(0), color='blue')
        plt.xlabel('Month')
        plt.ylabel(f'Temperature Maximum in {country_name} during {year}')
        plt.xticks(range(1, 13))

        plt.show()

def visualize_monthly():
    data_loc = r'Filtered Example Data\\Data Aggregation'

    file_list = os.listdir(data_loc)
    monthy_list = [file for file in file_list if 'monthly' in file]

    for country_file in monthy_list:
        key_dic = code_keydic()
        country, ext = country_file.split('.')
        country_name =  key_dic[country[-2:]]

        #group by year column, and divde into seperate dataframes based on year
        file_path = os.path.join(data_loc, country_file)
        country_df = pd.read_csv(file_path)
        year_grouped = country_df.groupby('Year')
        dfs_by_year = {}
        for year, group in year_grouped: dfs_by_year[year] = group.copy()

        year_slider = widgets.IntSlider(min=min(dfs_by_year.keys()), max=max(dfs_by_year.keys()), step=1, description='Year:', continuous_update=False)

        display(year_slider)
        
        widgets.interactive(plot_year, year=year_slider, country_name=widgets.fixed(country_name), dfs_by_year=widgets.fixed(dfs_by_year))

def visualize_yearly():
    pass

if __name__ == "__main__":
    #monthy_yearly_data()
    visualize_monthly()