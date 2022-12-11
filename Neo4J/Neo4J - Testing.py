# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 16:10:46 2022

@author: Markus Herre
"""

from neo4j import GraphDatabase
from union_find import UnionFind
import pandas as pd
import csv
from typing import Set
import multiprocessing as mp
import time


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


def multi_input_heuristic_iter(addr: str):
    # Initialize the set of all addresses to be empty
    all_addresses = set()
    
    # Initialize the queue of addresses to process with the given address
    queue = [addr]
    
    # Initialize the dictionary of visited addresses to be empty
    visited = {}
    
    # Use a while loop to repeatedly apply the multi_input_heuristic function until the queue is empty
    while queue:
        # Get the next address from the queue
        a = queue.pop()
        
        # If the address has already been visited, continue to the next iteration
        if a in visited:
            continue
        
        # Use the multi_input_heuristic function to get the set of addresses that were used together as inputs with the given address
        addresses = multi_input_heuristic(a)
        
        # Add the address to the dictionary of visited addresses
        visited[a] = True
        
        # Add the resulting set of addresses to the set of all addresses
        all_addresses.update(addresses)
        
        # Add the resulting addresses to the queue to be processed in the next iteration
        queue.extend(addresses)
    
    # Return the set of all addresses that were used together as inputs with the given address
    return all_addresses

def multi_input_heuristic_parallel(addr: str) -> Set[str]:
    # Set of addresses that have already been visited
    visited = set()
    # Set of addresses that were identified as potentially suspicious
    results = set()

    # Initialize the queue of addresses to process with the given address
    queue = [addr]
    
    # Initialize the number of active processes
    active_processes = 0

    while len(queue) > 0 or active_processes > 0:
        # If there are no more addresses in the queue and all processes have finished,
        # we are done and can return the results
        if len(queue) == 0 and active_processes == 0:
            return results

        # If there are no more addresses in the queue, but some processes are still active,
        # we can wait for them to finish and then continue processing the results
        if len(queue) == 0 and active_processes > 0:
            time.sleep(0.1)
            continue

        # Pop the next address from the queue
        addr = queue.pop()

        # Apply the multi-input heuristic to the address
        res = multi_input_heuristic(addr)

        # Add the address to the visited set
        visited.add(addr)

        # For each of the addresses returned by the multi-input heuristic,
        # apply the heuristic in parallel and add the address to the queue
        # if it has not already been visited
        for a in res:
            if a not in visited:
                queue.append(a)
                active_processes += 1
                mp.Process(target=multi_input_heuristic_iter, args=(a, results)).start()

        # Wait for all processes to finish and then add the returned addresses
        # to the results set
        active_processes -= 1
        results.update(res)

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
        result = multi_input_heuristic_iter(address)
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
  



def test(addr:str, n:int = 1):
    list1 = list()
    list2 = list()

    for i in range(n):
        # start = time.time()
        # a = multi_input_heuristic_iter(addr)
        # end = time.time()
        # list1.append(round(end - start,2))
        
        start = time.time()
        a = multi_input_heuristic_parallel(addr)
        print(a)
        end = time.time()
        list2.append(round(end - start,2))
        
    # print("Iter: " + str(sum(list1) / len(list1)))
    print("Parallel: " + str(sum(list2) / len(list2)))              

if __name__ == "__main__":
    
    # b = multi_input_heuristic_iter("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    # print(b)
    
    # a = multi_input_heuristic_rec("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi", 100)    
    # print(a)
    
    # a = get_all_Txs("1M6EFBLRZHWMttk2hsGK8ttf1AXBaxjm9r")
    # print(a)
    # f = get_related_addresses("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    # print(f)
    
    test("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi", 1)
    
    
    driver.close()
    
