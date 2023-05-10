# -*- coding: utf-8 -*-
"""
Created on Tue May  9 18:40:54 2023

@author: aquil
"""

#Machete de pandas

import pandas as pd

dfA = pd.DataFrame(
{"a" : [4, 5, 6],
"b" : [7, 8, 9],
"c" : [10, 11, 12]},
index = [1, 2, 3])

dfB = pd.DataFrame(
{"d" : [32, 41, 50],
"e" : [12, 21, 22],
"f" : [57, 11, 12]},
index = [1, 2, 3])

def

dfA.loc[:,]

dfA=dfA.drop(columns=["a"]) #borra col
dfA=dfA.drop(1) #borra fila