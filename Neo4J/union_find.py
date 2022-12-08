# -*- coding: utf-8 -*-
"""
Created on Thu Dec  8 11:52:54 2022

@author: Markus Herre
"""

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}
    
    def add(self, x):
        self.parent[x] = x
        self.rank[x] = 0
    
    def find(self, x):
        if x != self.parent[x]:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x, y):
        x_root = self.find(x)
        y_root = self.find(y)
        if x_root == y_root:
            return
        if self.rank[x_root] < self.rank[y_root]:
            self.parent[x_root] = y_root
        elif self.rank[x_root] > self.rank[y_root]:
            self.parent[y_root] = x_root
        else:
            self.parent[y_root] = x_root
            self.rank[x_root] += 1
    
    def elements(self):
        return set(self.parent.keys())