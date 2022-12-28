# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 16:10:46 2022

@author: Markus Herre
"""

from neo4j import GraphDatabase
import pandas as pd
import csv
import matplotlib.pyplot as plt
# import time



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
        
        a = len(all_addresses)
        # Add the resulting set of addresses to the set of all addresses
        all_addresses.update(addresses)
        b = len(all_addresses)
        print(str(len(all_addresses)) + " addresses found!", str((b - a)) + " new addreses found!")
    
        # Add the resulting addresses to the queue to be processed in the next iteration
        queue = ([addr for addr in addresses if addr not in visited]) + queue
        
    # Return the set of all addresses that were used together as inputs with the given address
    return all_addresses

import time

def multi_input_heuristic_iter2(addr: str):
    # Initialize the set of all addresses to be empty
    all_addresses = set()

    # Initialize the queue of addresses to process with the given address
    queue = [addr]
   
    # Initialize the dictionary of visited addresses to be empty
    visited = {}
    # Use a while loop to repeatedly apply the multi_input_heuristic function until the queue is empty
    while queue:
        # Initialize the count of new addresses found within the last minute to be 0
        new_addresses_count = 0

        # Get the current time
        start_time = time.time()

        # Use a while loop to process the addresses in the queue until the elapsed time is greater than 1 minute
        while queue and time.time() - start_time < 60:
            print(str(round(time.time() - start_time, 1)) + "Sekunden vergangen")
            # Get the next address from the queue
            a = queue.pop()
    
            # If the address has already been visited, continue to the next iteration
            if a in visited:
                continue
    
            # Use the multi_input_heuristic function to get the set of addresses that were used together as inputs with the given address
            addresses = multi_input_heuristic(a)

            # Add the address to the dictionary of visited addresses
            visited[a] = True
        
            a = len(all_addresses)
            # Add the resulting set of addresses to the set of all addresses
            all_addresses.update(addresses) 
            b = len(all_addresses)
            print(str(len(all_addresses)) + " addresses found!", str((b - a)) + " new addreses found!")
    
            # Add the resulting addresses to the queue to be processed in the next iteration
            queue = ([addr for addr in addresses if addr not in visited]) + queue

            # Increment the count of new addresses found within the last minute
            new_addresses_count += (b-a)
            # print(new_addresses_count)

        # If the sum of all addresses found within the last minute is less than 100, break the while loop
        if new_addresses_count < 100:
            break
        
    # Return the set of all addresses that were used together as inputs with the given address
    return all_addresses


# def test(addr:str, n:int = 1):
#     list1 = list()
#     list2 = list()
#     for i in range(round(n/2)):
#         start = time.time()
#         a = multi_input_heuristic_iter(addr)
#         end = time.time()
#         list1.append(round(end - start,2))
        
#         start = time.time()
#         b = multi_input_heuristic_iter2(addr)
#         end = time.time()
#         list2.append(round(end - start,2))
        
#     for i in range(round(n/2)):
#         start = time.time()
#         b = multi_input_heuristic_iter2(addr)
#         end = time.time()
#         list2.append(round(end - start,2))
        
#         start = time.time()
#         a = multi_input_heuristic_iter(addr)
#         end = time.time()
#         list1.append(round(end - start,2))
        
#     print(a)
#     print(b)
#     print("Iter: " + str(sum(list1) / len(list1)))
#     print("Iter2: " + str(sum(list2) / len(list2)))
    
def get_related_entities(addr: str):
    # Get all transactions for the given address
    txs = get_all_Txs(addr)
    # Filter the transactions to only include those with a positive result
    txs = txs[txs["result"] > 0]
    # Initialize a set to store the related addresses
    addresses = set()
    # For each of the transactions, apply get_all_Addr and add addresses with type "SENDS" to the set
    for _, tx in txs.iterrows():
        addr_df = get_all_Addr(tx["txid"])
        for address in addr_df[addr_df["type"] == "SENDS"]["address"]:
            addresses.add(address)
    # Initialize a list to store the heuristic results
    heuristic_results = []
    # Apply the recursive multi-input heuristic on each address in the set that has not been part of a heuristic result yet
    for address in addresses:
        if any(address in result for result in heuristic_results):
            continue
        print("Looking at: " + str(address))
        result = multi_input_heuristic_iter2(address)
        print("Found Entity: " + str(result))
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
    
    get_related_entities("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    
    # a = multi_input_heuristic_iter("3QQdfAaPhP1YqLYMBS59BqWjcpXjXVP1wi")
    # transactions = list()
    # for i in a:
    #     temp = get_all_Txs(i)
    #     buys = len(temp[temp["result"] > 0])
    #     sells = len(temp[temp["result"] < 0])
    #     transactions.append((i, buys, sells))

    # # Extract x and y values from the transactions list
    # x = [transaction[2] for transaction in transactions]
    # y = [transaction[1] for transaction in transactions]

    # # Plot the x and y values
    # plt.plot(x, y, 'bo')
    # plt.xlabel('Sells')
    # plt.ylabel('Buys')
    
    # # Add labels to the points
    # for i, transaction in enumerate(transactions):
    #     plt.text(x[i], y[i], transaction[0])
    
    # # Show the plot
    # plt.show()

    driver.close()
    
