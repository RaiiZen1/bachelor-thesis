# -*- coding: utf-8 -*-
"""
Created on Wed Oct 26 09:26:47 2022

@author: Markus Herre
"""

import pandas as pd
import time 
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as dts
import numpy as np
import csv


def get_txs_data(your_btc_address:str, page_size:int):
    transactions_url = 'https://blockchain.info/rawaddr/' + your_btc_address + '?limit=' + str(page_size)
    df = pd.read_json(transactions_url)
    time.sleep(10.1)
    return df["txs"]

def get_last_tx(df):
    return df[0]

def get_first_tx(df):
    return df[len(df) - 1]

def create_balance_to_time(your_btc_address:str, page_size:int):
    transactions = get_txs_data(your_btc_address, page_size)
    balances = list()
    dates = list()
    
    for i in transactions:
        balances.append(i["balance"])
        dates.append(dt.datetime.fromtimestamp(int(i["time"])))
    
    d = {'Date':dates,'Balance':balances}
    df = pd.DataFrame(d) 
    return df
    
def create_balance_to_time_plot():
    plt.rcParams["figure.figsize"] = (100,50)
    
    with open('addresses.csv', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        
    data.remove(data[0])
    
    df1 = create_balance_to_time("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi", 9999)
    ax = df1.plot(drawstyle="steps-post", x='Date', y='Balance', label = "3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    
    for i in data:
        df_temp = create_balance_to_time(i[1], 9999)
        df_temp.plot(drawstyle="steps-post", ax=ax, x='Date', y='Balance', label = i[1])

        
def cluster_addresses_year():
    year_2022 = list()
    year_2021 = list()
    year_2020 = list() 
    year_2020_to_2021 = list()
    year_2020_to_2022 = list()
    year_2021_to_2022 = list()
    
    with open('addresses.csv', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    data.remove(data[0])
    
    for i in data:
        transactions = get_txs_data(i[1], 9999)
        begin = get_first_tx(transactions)
        end = get_last_tx(transactions)
        begin_year = dt.datetime.fromtimestamp((int(begin["time"]))).year
        end_year = dt.datetime.fromtimestamp((int(end["time"]))).year
        
        if begin_year == 2020:
            if end_year == 2020:
                year_2020.append(i[1])
            elif end_year == 2021:
                year_2020_to_2021.append(i[1])
            else:
                year_2020_to_2022.append(i[1])
        elif begin_year == 2021:
            if end_year == 2021:
                year_2021.append(i[1])
            else:
                year_2021_to_2022.append(i[1])
        elif begin_year == 2022:
            year_2022.append(i[1])
            
    d = {'2020':year_2020,'2021':year_2021, "2022":year_2022, 
             "2020-2021":year_2020_to_2021, "2020-2022":year_2020_to_2022,
             "2021-2022":year_2021_to_2022}
    df = pd.DataFrame.from_dict(d, orient="index").T
    file = df.to_csv("addresses_clustered_year.csv")
    return df

def create_balance_to_time_plot_year(csv:str, year:str):
    plt.rcParams["figure.figsize"] = (100,50)
    
    df = pd.read_csv(csv)        
    
    try:
        df1 = create_balance_to_time(df[year][0], 9999)
        ax = df1.plot(drawstyle="steps-post", x='Date', y='Balance', label = df[year][0])
       
        for i in df[year]:
            try:
                df_temp = create_balance_to_time(i, 9999)
                df_temp.plot(drawstyle="steps-post", ax=ax, x='Date', y='Balance', label = i)  
            except:
                break;
    except:
        print("Fehler aufgetreten")
    
    
if __name__ == "__main__":
    # create_balance_to_time_plot()
    # cluster_addresses_year()
    create_balance_to_time_plot_year("addresses_clustered_year.csv", "2021-2022")
        

    
