# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 09:55:52 2022

@author: Markus Herre
"""
import pandas as pd
import time
from datetime import datetime
import csv

# Consumer Heuristic + Auszahlungspattern folgen (3QQ... -> A1 und 3QQ...)
def get_txs_data(your_btc_address:str, page_size:int):
    transactions_url = 'https://blockchain.info/rawaddr/' + your_btc_address + '?limit=' + str(page_size)
    df = pd.read_json(transactions_url)
    time.sleep(6)
    return df["txs"]

# Überprüft, ob eine Transaktion nur einen Input hat und nur zwei Outputs. Falls dies der Fall ist, wird überprüft,
# ob die Input Adresse auch als Output (Change) dient. Falls ja, geben wir True zurück, ansonsten nein. 
def is_tx_a_buy(tx:str):
    in_size = tx["vin_sz"]
    out_size = tx["vout_sz"]
    
    if in_size == 1 and out_size == 2:
        in_addr = tx["inputs"][0]["prev_out"]["addr"]
        for i in tx["out"]:
            if i["addr"] == in_addr:
                return True
        return False
    else:
        return False

def tx_patterns_to_csv(btc_addr:str):
    txs = get_txs_data(btc_addr, 99999)
    transactions = list()
    
    for i in txs:
        if is_tx_a_buy(i):
            transactions.append(i)        
    d = {"Txs":transactions}
    df = pd.DataFrame(d)
    df.to_csv(btc_addr + "_payout_pattern.csv")


# Für jede Transaktion, welche unserem Auszahlungspattern folgt, holen wir uns zuerst die potentielle BUY Adresse. 
# Diese Adresse wiederum überprüfen wir und schauen, ob sie unserem Consumer Heursitik Pattern entspricht
def consumer_heuristics(btc_addr:str):
    txs = tx_patterns_to_list(btc_addr) 
    transactions = list()
    addresses = set()
    a = len(txs) 
    b = 0
    for i in txs:    
        target = identify_target_addr(i, btc_addr)
        b += 1
        if is_target_consumer(target):
            transactions.append(i)
            addresses.add(target)   
            c = round((b / a) * 100, 2) 
            print("----------------------------------------LOG-FILE----------------------------------------")
            print("#Progress:\t\t" + str(b) + "/" + str(a) + " (" + str(c) + "%)")
            print("#Datetime:\t\t" + str(datetime.now()))
            print("#Viewed Address:\t" + btc_addr)
            print("#Found Transaction:\t" + i["hash"])
            print("#Consumer Address:\t" + target)
            print("----------------------------------------------------------------------------------------")
            print("\n")
            
    d = {"Txs":transactions}
    df = pd.DataFrame(d)
    df.to_csv("Consumer Heuristics/Transactions/2020/" + btc_addr + "_consumer_heuristic_tx.csv")
    
    addresses = list(addresses)
    d = {"Addr":addresses}
    df = pd.DataFrame(d)
    df.to_csv("Consumer Heuristics/Addresses/2020/" + btc_addr + "_consumer_heuristic_addr.csv")
            

# Wir beginnen mit dem Besorgen aller Transaktionen unserer BTC Adresse. Anschließend überprüfen wir jede Adresse,
# ob unsere BTC Adresse BTC erhalten hat nach dem Buy Pattern Schema (A1 -> A1 AND A2). Wir fügen alle Transaktionen,
# die diesem Schema folgen unserer Liste hinzu. Diese geben wir am Ende auch zurück.
def tx_patterns_to_list(btc_addr:str):
    txs = get_txs_data(btc_addr, 99999)
    pattern_txs = list()   
    for i in txs:
        if is_tx_a_buy(i):
            pattern_txs.append(i)        
    return pattern_txs

# Wir checken, ob die Output Adressen unsere Transaktion. Da unsere Outputs nie ghrößer als 2 sind, geben wir jene 
# Adresse zurück, welche nicht dem Input entspricht. Damit identifizieren wie potentielle ATM Buy Adressen
def identify_target_addr(tx:str, btc_addr:str):
    for i in tx["out"]:
        if not i["addr"] == btc_addr:
            target = i["addr"]
    return target
    
# Wir überprüfen, ob unsere Target Adresse Transaktionen enthält, wo die Output Größe größer 2 ist. Dies ist sehr
# untypisch für Consumer Wallets.
def is_target_consumer(target:str):
    txs = get_txs_data(target, 99999)
    for i in txs:
        if i["result"] < 0:
            if i["vout_sz"] > 2:
                return False
    return True
       
        
    
if __name__ == "__main__":
    # TRANSAKTIONEN NACH PATTERN GEFILTERT #########
    # with open('addresses.csv', newline='') as f:
    #     reader = csv.reader(f)
    #     data = list(reader)
    # data.remove(data[0])
    # for i in data:  
    #     tx_patterns_to_csv(i[1])
    #     print("Done with " + i[1])
    ################################################
    df = pd.read_csv("addresses_clustered_year.csv")
    addresses = df["2020"]
    for i in addresses:
        consumer_heuristics(i)
  
    