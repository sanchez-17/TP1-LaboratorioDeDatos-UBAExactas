# -*- coding: utf-8 -*-
"""
TP1LaboDatos.ipynb

Grupo: AltaData

Integrantes:Gaston Sanchez, Mariano Papaleo, Juan Pablo Hugo Aquilante

#Definicion de funciones a utilizar en el tp

"""
import pandas as pd
import re 
from unidecode import unidecode
from inline_sql import sql
import matplotlib.pyplot as plt

# =============================================================================
# DEFINICION DE FUNCIÓNES PARA ATOMIZAR COLUMNAS
# =============================================================================

def crear_y_añadir_fila(df,fila_old,col,valor):
    df.loc[len(df)] = df.loc[fila_old,:]
    df.at[len(df)-1, col] = valor

def atomizarFila(df,col,fila,string):
    valores_atomicos = df.loc[fila, col].split(string)
    for valor in valores_atomicos:
        crear_y_añadir_fila(df, fila,col, valor)
        #df.loc[len(df)] = df.loc[fila,:]
        #df.at[len(df)-1, col] = valor
    #borramos la fila original    
    df.drop([fila],inplace=True)
    df.reset_index(drop=True, inplace=True)

def atomizarColumna(df,col,string):
    for i in range(len(df)):
        atomizarFila(df,col,0,string)


def quitar_parentesis(string):
    #Quita todo lo que esta entre parentesis
    regex = r"\([^()]*\)"
    res = re.sub(regex, "", string)
    return res

def reemplazar(string,cadena,reemplazo):
    return string.replace(cadena,reemplazo)

def sacar_espacios_en_extremos(string):
    return string.strip()

def cuantos_nan(df):
    nan_cant = df.isna().sum()
    nan_porcentaje = (nan_cant / len(df)) * 100
    res = pd.concat([nan_cant, nan_porcentaje], axis=1, keys=['Recuento de NaN', 'Porcentaje de NaN'])
    res['Porcentaje de NaN'] = res['Porcentaje de NaN'].round(2).astype(str) + '%'
    res.index = df.columns
    print(res)

def extraer_pronombres(texto):
    # Expresión regular para encontrar pronombres
    patron = r'\b(para|de|con)\b'

    # Remover los pronombres del texto
    texto_sin_pronombres = re.sub(patron, '', texto, flags=re.IGNORECASE)
    
    # Retornar el texto sin pronombres
    return texto_sin_pronombres
    
def quitar_acentos(palabra):
    return unidecode(palabra)

# =============================================================================
# DEFINICION DE FUNCIÓN DE IMPRESIÓN EN PANTALLA
# =============================================================================
# Imprime en pantalla en un formato ordenado:
    # 1. Consigna
    # 2. Cada dataframe de la lista de dataframes de entrada
    # 3. Query
    # 4. Dataframe de salida
def imprimirEjercicio(consigna, listaDeDataframesDeEntrada, consultaSQL):
    
    print("# -----------------------------------------------------------------------------")
    print("# Consigna: ", consigna)
    print("# -----------------------------------------------------------------------------")
    print()
    for i in range(len(listaDeDataframesDeEntrada)):
        print("# Entrada 0",i,sep='')
        print("# -----------")
        print(listaDeDataframesDeEntrada[i])
        print()
    print("# SQL:")
    print("# ----")
    print(consultaSQL)
    print()
    print("# Salida:")
    print("# -------")
    print(sql^ consultaSQL)
    print()
    print("# -----------------------------------------------------------------------------")
    print("# -----------------------------------------------------------------------------")
    print()
    print()
# =============================================================================
# CONFIGURACION PARA DISEÑO
# =============================================================================
def configuraciones_diseño():
    plt.box(on=True)
    ax = plt.gca()
    ax.spines['top'].set_color('black')
    ax.spines['bottom'].set_color('black')
    ax.spines['left'].set_color('black')
    ax.spines['right'].set_color('black')
    ax.spines['top'].set_linewidth(2)
    ax.spines['bottom'].set_linewidth(2)
    ax.spines['left'].set_linewidth(2)
    ax.spines['right'].set_linewidth(2)
    #Ocultar numero de pixeles en eje X e Y
    plt.gca().set_xticks([])
    plt.gca().set_yticks([])
#%%