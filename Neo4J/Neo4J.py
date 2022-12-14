# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 16:10:46 2022

@author: Markus Herre
"""

from neo4j import GraphDatabase
import pandas as pd
import concurrent.futures
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
    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Address{address: $address})-[:SENDS]->(t:Transaction)
            WITH DISTINCT t
            MATCH (t)-[:SENDS]-(a2:Address)
            WITH DISTINCT a2
            RETURN a2
            """,
            address=addr,
        )
        # Convert the results into a set 
        data = {record["a2"]["address"] for record in result}       
    return data


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
        print(str(len(all_addresses)) + " addresses found!")
        
        # Add the resulting addresses to the queue to be processed in the next iteration
        queue.extend(addresses)
    
    # Return the set of all addresses that were used together as inputs with the given address
    return all_addresses


def multi_input_heuristic_iter2(addr: str):
    # Initialize the set of all addresses to be empty
    results = set()
    visited = set()
    set_A = multi_input_heuristic(addr)
    
    # Keep applying multi_input_heuristic() to the set_A until no new addresses are found
    while set_A:
        visited = visited.union(set_A)
        # Use a ProcessPoolExecutor to concurrently apply multi_input_heuristic() to each element in set_A
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for set_B in executor.map(multi_input_heuristic, set_A):
                results = results.union(set_B)

        # Set set_A to be the new addresses found in the previous step
        set_A = results.difference(visited)
    
    return results


def test(addr:str, n:int = 1):
    list1 = list()
    list2 = list()
    for i in range(n):
        start = time.time()
        a = multi_input_heuristic_iter(addr)
        end = time.time()
        list1.append(round(end - start,2))
        
        # start = time.time()
        # b = multi_input_heuristic_iter2(addr)
        # end = time.time()
        # list2.append(round(end - start,2))
        
    print(a)
    # print(b)
    print("Iter: " + str(sum(list1) / len(list1)))
    # print("Iter2: " + str(sum(list2) / len(list2)))
    


if __name__ == "__main__":

    # a  = get_all_Txs("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    # print(a.iloc[0]) 

    # b = get_all_Addr("92098f93bd77a746e9387f8871cc6186aada265b9d6103102cf8982f172bfc84")
    # print(b)
    
    # c = multi_input_heuristic_iter("1FjKzGEyh9au36Zkwb3THV5k6ySXrpfVLh")
    # print(c) 
    
    # c = multi_input_heuristic_parallel("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    # print(c) 
    
    test("31wrujVVhf92puvwe4uE1cDgKNc4gXuQRz", 1)
    
    # c = multi_input_heuristic("36CVcEXqdGik7y4ySmyA4V3Sfh7GVgUJeo")
    # print(c)

    
    driver.close()
    
