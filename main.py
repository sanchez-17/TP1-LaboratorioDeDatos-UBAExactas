#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

#%% 
"--------------Tratamiento de NaNs-------------------"
#En columna rubro y productos vemos los casos particulares
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
df1.loc[mask, ['rubro', 'productos']] = [sin_definir for col in ['rubro', 'productos']]
#fila 879 tiene mas de un producto, a definir mas adelante
df1.loc[879,'rubro'] = sin_definir
#Hay registros con error de tipo: agicultura
df1.loc[df1.rubro == "AGICULTURA","rubro"] = "AGRICULTURA"
#Hay campos con el valor "SIN DEFINIR",, los cambiamos a "INDEFINIDO"
df1.loc[df1.rubro == "SIN DEFINIR","rubro"] = sin_definir
#Aquellas filas en donde contienen puntos. En algunas 
#funcionan como separadores, en otras no aportan nada
terminan_en_punto = df1.rubro.str.endswith('.')
df1.loc[terminan_en_punto,'rubro'] = df1.loc[terminan_en_punto,'rubro'].str.replace(".", "")
#Veo cuantas siguen con puntos como separadores:2,mas adelante los separamos
f = df1.rubro.str.contains("\.")
aux=df1.loc[f,"rubro"]
#%%
#Sacamos los nan de columna productos
#df1.dropna(inplace=True,subset=["productos"])
prod_con_par = df1.loc[df1.productos.str.contains("\(")]
"""-------Columna productos-------------"""
#Veo los NaN

col = "productos"
#Spliteamos por comas
atomizarColumna(df1,col,', ')
#Spliteamos por ;
atomizarColumna(df1,col,';')
#Spliteamos por -
atomizarColumna(df1,col,'-')
atomizarColumna(df1,col,' + ')
atomizarColumna(df1,col,' ? ')
#%%
#Es posible que tengamos problemas al separar por " Y ". La tarea no es trivial. VER
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
#Todos los productos que contengan la palabra campo, natural reemplazar el valor por inculto
filtro = df1.productos.str.contains("CAMPO") or df1.productos.str.contains("MONTE") or df1.productos.str.contains("PASTURAS")
df1.loc[filtro,"rubro"] = "INCULTO"
#%%
"""-------------------------------COLUMNA RUBRO--------------------------------------------"""


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
#%%
#Vemos la cantidad de valores unicos para poder atomizar aquellas tuplas que asi lo requieran
aux = df1.rubro.value_counts()
"""
Habra que separar por:
    .
    /
    " Y "
    ,
    -
    ;
Ademas sacar los puntos
y por ultimo sacar los espacios(el primer espacio y ultimo)

"""

col="rubro"
atomizarColumna(df1,col,'.')
#Verifico
df1.rubro.str.contains("\.").sum() #0

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
df1[df1.rubro.str.startswith(" ")].rubro.count() #0
#%%
"""--------------Columna establecimiento-------------------"""
df1.loc[df1.establecimiento == "NC","establecimiento"] = sin_definir
