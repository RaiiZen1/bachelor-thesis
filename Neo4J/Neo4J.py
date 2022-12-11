# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 16:10:46 2022

@author: Markus Herre
"""

from neo4j import GraphDatabase
import pandas as pd
import multiprocessing
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
    return addresses
    
def multi_input_heuristic_iter(addr: str):
    # Initialize the set of all addresses to be empty
    all_addresses = set()
    # Initialize the queue of addresses to process with the given address
    queue = [addr] 
    # Initialize the set of visited addresses to be empty
    visited = set()   
    # Use a while loop to repeatedly apply the multi_input_heuristic function until the queue is empty
    while queue:
        # Get the next address from the queue
        a = queue.pop()  
        # If the address has already been visited, continue to the next iteration
        if a in visited:
            continue        
        # Use the multi_input_heuristic function to get the set of addresses that were used together as inputs with the given address
        addresses = multi_input_heuristic(a)   
        # Add the address to the set of visited addresses
        visited.add(a)      
        # Add the resulting set of addresses to the set of all addresses
        all_addresses.update(addresses)     
        # Add the resulting addresses to the queue to be processed in the next iteration
        queue.extend(addresses)  
    # Return the set of all addresses that were used together as inputs with the given address
    return all_addresses

def multi_input_heuristic_parallel(addr: str):
    # Initialize the set of all addresses to be empty
    all_addresses = set()
    
    # Initialize the queue of addresses to process with the given address
    queue = [addr] 

    # Create a pool of workers
    with multiprocessing.Pool() as pool:
        while queue:
            # Divide the queue into chunks and process each chunk in parallel
            results = pool.map(multi_input_heuristic, queue)
            
            # Flatten the results into a single list of addresses
            addresses = [address for result in results for address in result]
            
            # Update the set of all addresses with the new addresses
            all_addresses.update(addresses)
            
            # Update the queue with the new addresses
            queue = [address for address in addresses if address not in all_addresses]

    return all_addresses

def test(addr:str, n:int = 1):
    list1 = list()
    list2 = list()

    for i in range(n):
        start = time.time()
        multi_input_heuristic_iter(addr)
        end = time.time()
        list1.append(round(end - start,2))
        
        start = time.time()
        multi_input_heuristic_parallel(addr)
        end = time.time()
        list2.append(round(end - start,2))
        
    print("Iter: " + str(sum(list1) / len(list1)))
    print("Parallel: " + str(sum(list2) / len(list2)))
    


if __name__ == "__main__":

    # a  = get_all_Txs("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    # print(a.iloc[0]) 

    # b = get_all_Addr("92098f93bd77a746e9387f8871cc6186aada265b9d6103102cf8982f172bfc84")
    # print(b)
    
    # c = multi_input_heuristic_iter("1FjKzGEyh9au36Zkwb3THV5k6ySXrpfVLh")
    # print(c) 
    
    # c = multi_input_heuristic_parallel("1FjKzGEyh9au36Zkwb3THV5k6ySXrpfVLh")
    # print(c) 
    
    test("31oZ73ytkMfT5eHnCwx8cLqfxx2Ds8WjKg", 5)

    
    driver.close()
    
