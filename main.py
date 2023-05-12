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

#%%
#df1['productos'].value_counts()

#a=df1.loc[df1['productos'].str.contains('HORTICULTURA',na=False)]


def atomizarFila(df,col,fila,string):
    valores_atomicos = df.loc[fila, col].split(string)
    for valor in valores_atomicos:
        df.at[len(df),:] = df.loc[fila,:]
        df.at[len(df)-1, col] = valor
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
#%%
#Sacamos los nan de columna productos
df1.dropna(inplace=True,subset=["productos"])

"""-------Columna productos-------------"""
col = "productos"
#Spliteamos por comas
atomizarColumna(df1,'productos',', ')
#Spliteamos por y
atomizarColumna(df1,'productos',' Y ')
#Spliteamos por ;
atomizarColumna(df1,'productos',';')
#Spliteamos por -
atomizarColumna(df1,'productos','-')
#%%
#df1['productos'] = df1['productos'].apply(quitar_punto,args=('.'))

def cambiar_todo_string(string,viejo,nuevo):
    if string.str.contains(viejo):
        return nuevo

#df1.loc[df['productos']]



""" separar """

df_productos = df1.loc[:,['productos','razón social','establecimiento']]
df_productos.dropna(inplace=True,subset="productos")
atomizarColumna(df_productos,'productos', ',')
df1.drop('productos',axis=1)
"""
Rubro separar por:
    ?
    +
"""
#%%
"""COLUMNA RUBRO"""
#Vemos los casos particulares
sin_definir="INDEFINIDO"
#Veamos si para un producto."CAMPO INCULTO" aplica un valor en especifico para 
#la columna rubro
#Filtramos nans con mascara para evitar mensaje de error:
mask = ~df1.rubro.isna()
df1_rubro_productos_sinNan = df1.loc[df1.productos.str.contains("INCULTO") & mask,["productos","rubro"]]
#Vemos que INCULTO refiere al rubro AGRICULTURA, redefinimos los nan
df1_producto_inculto_conNan = df1.loc[df1.rubro.isna(),["rubro","productos"]]
df1.at[853, 'rubro'] = sin_definir
df1.at[908, 'rubro'] = sin_definir
df1.at[908, 'productos'] = sin_definir
#luego aquellas las que tanto en productos y rubro tengo nan:
mask = df1.rubro.isna() & df1.productos.isna()
cols = ['rubro', 'productos']
df1.loc[mask, cols] = [sin_definir for col in cols]
#fila 879 tiene mas de un producto, a definir mas adelante
df1.loc[879,'rubro'] = sin_definir
#Hay registro con error de tipo: agicultura

    
#%%
#Atomizando columna rubro
#Genero serie booleana para encontrar lo que buscamos
df1.rubro.str.contains("SIN DEFINIR").value_counts()
#Hay 109 registros en rubro sin definir
df1_rubro_sin_definir = df1.loc[df1.rubro.str.contains("SIN DEFINIR")]
#Al parecer estos registros tienen en la columna categoria_desc el valor "Comercializadores", veamos si pasa con todos los registros
filtro = df1.categoria_desc.str.contains("Comercializadores")
df1_categoria_comercializadores = df1.loc[filtro]
#¿Es cierto que para todas las tuplas en donde categoria_desc="Comercializadores" pasa que rubro="SIN DEFINIR"?
len(df1_rubro_sin_definir) == len(df1_categoria_comercializadores)
#Es verdad!

#Veo los registros con el valor "OTROS " en columna rubro
bool2=df1.rubro.str.contains("OTROS")
aux3=df1.loc[bool2]
#Hay 7 registros

#Vemos la cantidad de valores unicos para poder atomizar aquellas tuplas que asi lo requieran
c = df1.rubro.value_counts()
"""
Habra que separar por:
    /
    " Y "
    ,
    -
    ;
Ademas sacar los puntos
y por ultimo sacar los espacios(el primer espacio y ultimo)

"""

col="rubro"
atomizarColumna(df1,col,'/')
#Verifico
df1.rubro.str.contains("/").sum() #0

atomizarColumna(df1,col,',')
df1.rubro.str.contains(',').sum() #0

atomizarColumna(df1,col,';')
df1.rubro.str.contains(';').sum() #0

atomizarColumna(df1,col,'-')
df1.rubro.str.contains('-').sum() #0

df1[col] = df1[col].apply(sacar_espacios_en_extremos)
a=df1[df1.rubro.str.startswith(" ")].rubro.count() #0
df1[col] = df1[col].apply(quitar_punto,args=('.'))
#Es posible que tengamos problemas al separar por " Y "
bool4 = df1.rubro.str.contains(" Y ")
aux4 = df1.loc[bool4].rubro

bool5 = df1.rubro.str.contains("EMPAQUE")
aux5 = df1.loc[bool5,["rubro","productos"]]

bool6 = df1.rubro.str.contains(" Y ")
aux5 = df1.loc[bool4].rubro

##Bueno, separamos por " Y "


#Para todos aquellos productos que tengan "INCULTO", definir en columna rubro AGRICULTURA
filtro = df1.productos.str.contains("INCULTO")
df1.loc[filtro,'rubro'] = "AGRICULTURA"
#%%
df1.productos.str.contains("VID").count
a=df1.productos=="UVAS"
a.sum()
b=df1.loc[df1.productos.str.contains("VINO"),["rubro","productos"]]
#42 contiene UVAS
c=df1.loc[df1.productos.str.contains("VID"),["rubro","productos"]]
c=df1.loc[df1.productos.str.contains("VID")]

#110 contiene VID
d=df1.loc[df1.productos.str.contains("ACOPIO"),["rubro","productos"]]
#%%
df_provincia = df1[["provincia_id","provincia"]].drop_duplicates().reset_index(drop=True)
df_pais = df1[["pais_id","pais"]].drop_duplicates().reset_index(drop=True)

#%%
df_provi = df[['D_NOMBRE', 'D_NUMERO', 'D_CUIL']]
df_areas = df['D_AREAS'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).reset_index(name='D_AREAS')

#%%
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
#%%
s = "EXTRACCION DE FRUTAS Y VERDURAS"
