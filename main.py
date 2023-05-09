#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  8 09:14:56 2023

@author: clinux01
"""
import pandas as pd
import re 


padron = './dataframes/padron-de-operadores-organicos-certificados.csv'
salario = './dataframes/w_median_depto_priv_clae2.csv'
loccensales = './dataframes/localidades-censales.csv'
dicdepto = './dataframes/diccionario_cod_depto.csv'
dicclase = './dataframes/diccionario_clae2.csv'

df1 = pd.read_csv(padron,encoding = 'windows-1258')
df2 = pd.read_csv(salario)
df3 = pd.read_csv(loccensales)
df4 = pd.read_csv(dicdepto)
df5 = pd.read_csv(dicclase)


#df1['productos'].value_counts()

#a=df1.loc[df1['productos'].str.contains('HORTICULTURA',na=False)]


def atomizarFila(df,col,fila,string):
    fila2 = df.loc[fila,:]
    aux=df.loc[fila,col].split(string)
    for elem in aux:
        fila_nueva= fila2
        fila_nueva[col] = elem
        df.loc[len(df)]= fila_nueva
    #borramos la fila original
    df.drop([fila],inplace=True)
    df.reset_index(drop=True, inplace=True)
    


def atomizarColumna(df,col,string):
    rango = len(df)
    i = 0
    while i < rango:
        atomizarFila(df,col,0,string)
        i += 1

def quitar_parentesis(string):
    regex = r"\([^()]*\)"
    res = re.sub(regex, "", string)
    return res

def quitar_punto(string):
    return string.replace(".", "")
        
def quitar_dospuntos(string):
    return string.replace(":", "")

#df1.dropna(inplace=True,subset="productos")

#-------Columna productos-------------
col = "productos"
#Spliteamos por comas
atomizarColumna(df1,'productos',', ')
#Spliteamos por y
atomizarColumna(df1,'productos',' Y ')
#Spliteamos por ;
atomizarColumna(df1,'productos',';')
#Spliteamos por -
atomizarColumna(df1,'productos','-')

df1['productos'] = df1['productos'].apply(quitar_punto)

def cambiar_todo_string(string,viejo,nuevo):
    if string.str.contains(viejo):
        return nuevo

#df1.loc[df['productos']]


