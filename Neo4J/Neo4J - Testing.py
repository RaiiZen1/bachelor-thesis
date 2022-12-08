# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 16:10:46 2022

@author: Markus Herre
"""

from neo4j import GraphDatabase
from union_find import UnionFind
import pandas as pd
import csv




# Replace "bolt_uri" and "password" with the bolt URI and password for your Neo4J database
bolt_uri = "neo4j://127.0.0.1:7687"
user = "mherre"
password = "B1tc01n"

# Create a driver instance to connect to the database
driver = GraphDatabase.driver(bolt_uri, auth=(user, password))

# Connect to the database and run a simple query that returns all transactions of a given address 
def get_all_Txs(addr:str):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Address {address: $address})-[s:SENDS|RECEIVES]-(t:Transaction)
            WITH DISTINCT t, SUM(CASE WHEN s:SENDS THEN s.value * (-1) ELSE s.value END) AS total
            RETURN t, total
            """,
            address=addr,
        )
        # Convert the results into a dataframe and sort them by date
        data = [
            {
        "date": record["t"]["date"],
        "inDegree": record["t"]["inDegree"],
        "txid": record["t"]["txid"],
        "outDegree": record["t"]["outDegree"],
        "inSum": record["t"]["inSum"],
        "outSum": record["t"]["outSum"],
        "fee": record["t"]["inSum"] - record["t"]["outSum"],
        "result": record["total"],
            }
            for record in result
        ]    
        df = pd.DataFrame(data)
        df = df[['txid', 'date', 'inDegree', 'outDegree', "fee", 'result']]
        df = df.sort_values("date")
        df = df.reset_index(drop=True)
    return df

# Connect to the database and run a simple query that returns all addresses that were used in a 
# particular transaction 
def get_all_Addr(tx:str):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Address)-[r]-(t:Transaction {txid: $txid})
            RETURN a, r, TYPE(r) AS ty
            """,
            txid=tx,
        )
        # Convert the results into a dataframe and sort them by inDegree
        data = [
            {
        "address": record["a"]["address"],
        "inDegree": record["a"]["inDegree"],
        "outDegree": record["a"]["outDegree"],
        "type": record["ty"],
        "amount": record["r"]["value"]
            }
            for record in result
        ]    
        df = pd.DataFrame(data)      
    return df

# This method applies the multi input heuristic on a given address
def multi_input_heuristic(addr:str):
    txs = get_all_Txs(addr)
    # Filter the transactions to only include those with a negative result
    txs = txs[txs["result"] < 0]
    # Get the set of addresses that were used as inputs in transactions with multiple inputs
    addresses = set()
    for _, tx in txs[txs["inDegree"] > 1].iterrows():
        addr_df = get_all_Addr(tx["txid"])   
        addresses.update(set(addr_df[addr_df["type"] == "SENDS"]["address"]))
    # If the set is empty, add the given address to the set
    if not addresses:
        addresses.add(addr)
    return addresses

    
def multi_input_heuristic_rec(addr: str):
    # Initialize the set of all addresses to be empty
    all_addresses = set()
    
    # Initialize the set of visited addresses to be empty
    visited = set()
    
    # Define a helper function to recursively apply the multi input heuristic on the given address
    def helper(addr: str):
        # If the address has already been visited, return
        if addr in visited:
            return
        
        # Use the multi_input_heuristic function to get the set of addresses that were used together as inputs with the given address
        addresses = multi_input_heuristic(addr)
        
        # Add the address to the set of visited addresses
        visited.add(addr)
        
        # Add the resulting set of addresses to the set of all addresses
        all_addresses.update(addresses)
        
        # Recursively apply the helper function on each of the resulting addresses
        for a in addresses:
            helper(a)
    
    # Call the helper function to recursively apply the multi input heuristic on the given address
    helper(addr)
    
    # Return the set of all addresses that were used together as inputs with the given address
    return all_addresses


def get_related_addresses(addr: str):
    # Get all transactions for the given address
    txs = get_all_Txs(addr)
    # Filter the transactions to only include those with a positive result
    txs = txs[txs["result"] > 0]
    # Initialize a union-find data structure to store the related addresses
    uf = UnionFind()
    # For each of the transactions, apply get_all_Addr and add addresses with type "SENDS" to the set
    for _, tx in txs.iterrows():
        addr_df = get_all_Addr(tx["txid"])
        for address in addr_df[addr_df["type"] == "SENDS"]["address"]:
            uf.add(address)
    # Apply the recursive multi-input heuristic on each address in the set
    heuristic_results = []
    for address in uf.elements():  
        print("Looking at: " + str(address))
        # Skip the address if it is already part of a heuristic result
        if any(address in result for result in heuristic_results):
            continue
        result = multi_input_heuristic_rec(address)
        # Add the result to the list of heuristic results
        print("Found Entity: ")
        print(result)
        print("####################################################")
        heuristic_results.append(result)
    # Save the heuristic results to a file
    with open("heuristic_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entities", "addresses"])
        for i, result in enumerate(heuristic_results):
            row = ["Entity {}:".format(i)]
            for address in result:
                row.append("{}".format(address))
            writer.writerows([row])
                

if __name__ == "__main__":

    # a = multi_input_heuristic_rec("1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s")    
    # print(a)
    
    # a = get_all_Txs("1M6EFBLRZHWMttk2hsGK8ttf1AXBaxjm9r")
    # print(a)
    f = get_related_addresses("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    print(f)
    
    
    driver.close()
    
