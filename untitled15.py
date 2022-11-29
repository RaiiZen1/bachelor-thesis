# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 17:46:31 2022

@author: Markus Herre
"""
import pandas as pd
import time 


def get_txs_data(btc_addr:str, page_size:int):
    transactions_url = 'https://blockchain.info/rawaddr/' + btc_addr + '?limit=' + str(page_size)
    df = pd.read_json(transactions_url)
    time.sleep(6)
    return df["txs"]

def create_wallet_set(btc_addr:str):
    df = get_txs_data(btc_addr, 9999)
    wallets = set()
    for i in df:
        if i["result"] < 0 and i["vin_sz"] > 1:
            for j in i["inputs"]:
                wallets.add(j["prev_out"]["addr"])
    return wallets

def multi_input_heuristic(your_btc_address:str, all_addresses:set):
    addresses = create_wallet_set(your_btc_address)
    length_bef = len(all_addresses)
    all_addresses.update(addresses)
    length_aft = len(all_addresses)
    if length_bef == length_aft:
        return all_addresses  
    else:
        for i in addresses:
           multi_input_heuristic(i, all_addresses)
               
def get_buy_addr(btc_addr:str):
    df = get_txs_data(btc_addr, 9999)
    buy_list = set()
    for i in df:
        if i["result"] > 0:
            for j in i["inputs"]:
                buy_list.add(j["prev_out"]["addr"])
    return buy_list
         
            
if __name__ == "__main__":
    addresses = get_buy_addr("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    all_entities = list()
    n = 0
    for i in addresses:
        set1 = set()
        multi_input_heuristic(i, set1)
        if len(set1) == 0:
            all_entities.append([n, i])
        else:
            all_entities.append([n, set1])
        n += 1
        print(str(n) + "/" + str(len(addresses)))
    df = pd.DataFrame(all_entities, columns =['Entity', 'Addresses']) 
    df.to_csv("Buying Entities.csv")