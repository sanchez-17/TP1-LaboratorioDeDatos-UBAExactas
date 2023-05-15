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
def atomizarFila(df,col,fila,string):
    valores_atomicos = df.loc[fila, col].split(string)
    for valor in valores_atomicos:
        #df.at[len(df),:] = df.loc[fila,:]
        df.loc[len(df)] = df.loc[fila,:]
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

def sacar_espacios_columna(df,columna):
    df[columna] = df[columna].str.strip()

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
cols = ['rubro', 'productos']
df1.loc[mask, cols] = [sin_definir for col in cols]
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

"""-------Columna productos-------------"""
#Veo los NaN

col = "productos"
#Spliteamos por comas
atomizarColumna(df1,col,', ')
#Spliteamos por y
atomizarColumna(df1,col,' Y ')###
#Spliteamos por ;
atomizarColumna(df1,col,';')
#Spliteamos por -
atomizarColumna(df1,col,'-')
#%%

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
productos separar por:
    ?
    +
"""
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
#%%
df2.rename(columns={'id_provincia_indec': 'provincia_id'}, inplace=True)

""" Como saber cuantos registros de la columna 'w_median' son inferiores a 0 """

df2_salario_negativo = df2.loc[df2['w_median'] < 0] 


""" Y para saber cuanto representan esa cantidad respecto del total simplemente calculamos"""

len(df2_salario_negativo)/len(df2)*100


""" Luego para ubicar los valores NaN en la tabla"""

NaN_filas = df2[df2.isna().any(axis=1)] # Con esto sabemos cuantas filas tienen al menos 1 NaN
NaN_columnas = df2.columns[df2.isna().any()].tolist() # Con esto sabemos que columnas tienen NaN

# Y ahora podemos fijarnos si los NaN aparecen simultaneamente en ambas columnas o si se dividen apareciendo a veces
# en una y a veces en la otra.

dptoNaN = df2['codigo_departamento_indec'].isna() # Retorna 9156
provNaN = df2['id_provincia_indec'].isna() # Retorna 9156

# Pero con esto no nos alcanza asi que por último chequeamos si cada vez que aparece False o True en dptoNaN 
# se corresponde con los False o True de provNaN. Eso daria una serie de Pandas llena de valores True. Y si el sum()
# (que solo cuenta los True) es igual a la len del dataframe original entonces cada vez que aparece NaN en una aparece
# en la otra.

(dptoNaN == provNaN).sum() == len(df2)  # Esto da True.

#%%

#TRATAMIENTO DE NANS
df3.loc[df3.departamento_nombre.isna(),"departamento_nombre"]=sin_definir
df3.loc[df3.funcion.isna(),"funcion"]=sin_definir
df3.loc[df3.municipio_nombre.isna(),"municipio_nombre"]=sin_definir
df3.loc[df3.municipio_id.isna(),"municipio_id"]=-99
df3.loc[df3.departamento_id.isna(),"departamento_id"]=-99

df3 = df3.rename(columns ={'provincia_nombre':'nombre_provincia'})
df3 = df3.rename(columns ={'municipio_nombre':'nombre_municipio'})
df3 = df3.rename(columns ={'departamento_nombre':'nombre_departamento'})
df3_dict=df3

#PARTIMOS PROVINCIA
df3_provincia = df3[['provincia_id','nombre_provincia']].drop_duplicates().reset_index(drop =True)
df3_provincia==df4_provincia
df3_dict = df3_dict.drop('nombre_provincia',axis=1)

#PARTIMOS MUNICIPIO
df3_municipio = df3[['municipio_id', 'nombre_municipio']].drop_duplicates().reset_index(drop=True)
df3_dict = df3_dict.drop('nombre_municipio',axis=1)

#PARTIMOS DEPARTAMENTO
df3_departamento = df3[['departamento_id', 'nombre_departamento']].drop_duplicates().reset_index(drop=True)
df3_dict = df3_dict.drop('nombre_departamento',axis=1)

#PARTIMOS LOCALIDAD
df3_localidad = df3[['id', 'nombre','funcion','centroide_lat','centroide_lon','categoria','fuente']].drop_duplicates().reset_index(drop=True)
df3_dict = df3_dict.drop(labels=['nombre','funcion','centroide_lat','centroide_lon','categoria','fuente'],axis=1)
#%%

# PARTIENDO EL DATAFRAME 4 

# PRIMERO RENOMBRAMOS 

df4  = df4.rename(columns ={'codigo_departamento_indec': 'codigo_departamento'})
df4 = df4.rename(columns ={'nombre_departamento_indec':'nombre_departamento'})
df4  = df4.rename(columns ={'id_provincia_indec': 'provincia_id'})
df4 = df4.rename(columns ={'nombre_provincia_indec':'nombre_provincia'})

#PARTIMOS DEPARTAMENTO
df4_departamento = df4[['codigo_departamento', 'nombre_departamento']].drop_duplicates().reset_index(drop=True)

#PARTIMOS PROVINCIA
df4_provincia = df4[['provincia_id','nombre_provincia']].drop_duplicates().reset_index(drop =True)

# df4_dict es la versión normalizada de df4 que se conecta mediante la PK con df4_departamento y df4_provincia
df4_dict = df4.drop('nombre_departamento',axis=1)
df4_dict = df4_dict.drop('nombre_provincia',axis=1)

#%%

# EL df5 tiene un valor NaN cuando 'clae2' es igual a 999. Esto es debido a que el valor 'OTROS' no tiene asignada una
# letra. Por lo que decidimos asignarle la Z.

df5.loc[85,'letra'] = 'Z'

# PARTIENDO EL DATAFRAME 5

# PARTIMOS CLAE2_DESC

df5_clae2 = df5[['clae2', 'clae2_desc']].drop_duplicates().reset_index(drop=True)

# PARTIMOS LETRA_DESC

df5_letra = df5[['letra', 'letra_desc']].drop_duplicates().reset_index(drop=True)

# df5_dict es la versión normalizada de df5 que se conecta mediante la PK con df5_clae2 y df5_letra
df5_dict = df5.drop('clae2_desc',axis=1)
df5_dict = df5_dict.drop('letra_desc',axis=1)

""" es dificil normalizar df5_clae2 ya que las descripciones estan pensadas como string y no para separarse en valores atomizados.
# NORMALIZAMOS df5_clae2

atomizarColumna(df5_clae2,'clae2_desc', ', ')
atomizarColumna(df5_clae2,'clae2_desc',' y ')
"""

# NORMALIZAMOS df5_letra

# Para ello hace falta renombrar gran parte de las descripciones ya que se pensaron para estar como un string
# pero nosotros queremos atomizar cada atributo para que esten en 1FN en vez de pensarlos como un string.

df5_letra = df5_letra.replace({'EXPLOTACION DE MINAS Y CANTERAS' : 'EXPLOTACIÓN DE MINAS Y EXPLOTACIÓN DE CANTERAS',
                               ' SUMINISTRO DE ELECTRICIDAD, GAS, VAPOR Y AIRE ACONDICIONADO' : 'SUMINISTRO DE ELECTRICIDAD,SUMINISTRO DE GAS, SUMINISTRO DE VAPOR, SUMINISTRO DE AIRE ACONDICIONADO',
                            ' SUMINISTRO DE AGUA; CLOACAS; GESTIÓN DE RESIDUOS Y RECUPERACIÓN DE MATERIALES Y SANEAMIENTO PUBLICO':'SUMINISTRO DE AGUA, CLOACAS, GESTIÓN DE RESIDUOS, RECUPERACIÓN DE MATERIALES, SANEAMIENTO PÚBLICO',
                        	' COMERCIO AL POR MAYOR Y AL POR MENOR; REPARACIÓN DE VEHÍCULOS AUTOMOTORES Y MOTOCICLETAS' :'COMERCIO AL POR MAYOR, COMERCIO AL POR MENOR, REPARACIÓN DE VEHÍCULOS AUTOMOTORES, REPARACIÓN DE MOTOCICLETAS',
                        	' SERVICIO DE TRANSPORTE Y ALMACENAMIENTO':'SERVICIO DE TRANSPORTE, SERVICIO DE ALMACENAMIENTO',
                        	' SERVICIOS PROFESIONALES, CIENTÍFICOS Y TÉCNICOS': 'SERVICIOS PROFESIONALES, SERVICIOS CIENTÍFICOS, SERVICIOS TÉCNICOS',
                        	' SERVICIOS  ARTÍSTICOS, CULTURALES, DEPORTIVOS  Y DE ESPARCIMIENTO': 'SERVICIOS ARTÍSTICOS, SERVICIOS CULTURALES, SERVICIOS DEPORTIVOS, SERVICIOS DE ESPARCIMIENTO',
                            })

# Una vez renombrado todo a mano porque hacer una función tardaria demasiado. Podemos atomizar

atomizarColumna(df5_letra,'letra_desc', ', ')
atomizarColumna(df5_letra,'letra_desc',' y ')
sacar_espacios_columna(df5_letra,'letra_desc') # esta función elimina los espacios al principio y final de las palabras gracias a la función strip




#%%
#-------JUAN PABLO

df1_resultado_fails=df1_resultado[df1_resultado.departamento_id.isna()]
#df3.loc[df3.provincia=="TIERRA DEL FUEGO","provincia"]="TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR"
df3.loc[df3.provincia=="TUCUMÁN","provincia"]="TUCUMAN"
df3.loc[df3.provincia=="RÍO NEGRO","provincia"]="RIO NEGRO"
df3.loc[df3.provincia=="NEUQUÉN","provincia"]="NEUQUEN"
df3.loc[df3.provincia=="ENTRE RÍOS","provincia"]="ENTRE RIOS"
df3.loc[df3.provincia=="CÓRDOBA","provincia"]="CORDOBA"
df3.loc[df3.provincia=="CIUDAD AUTÓNOMA DE BUENOS AIRES","provincia"]="CABA"

df1.loc[df1.provincia=="CIUDAD AUTONOMA BUENOS AIRES","provincia"]="CABA"
df1.loc[df1.departamento=="CIUDAD AUTONOMA BUENOS AIRES","departamento"]="CABA"
df1.loc[df1.provincia=="CIUDAD AUTONOMA BUENOS AIRES" | df1.provincia=="CIUDAD AUTONOMA BUENOS AIRES" ,["provincia","departamento"]]=["CABA","CABA"]

df3_depYProv=df3[['provincia_id','provincia','departamento_id', 'departamento']].drop_duplicates().reset_index(drop=True)
df1_depYProv=df1[['provincia_id','provincia','departamento']].drop_duplicates().reset_index(drop=True)
df1_corregido.insert(4,'departamento_id',df1_corregido.pop('departamento_id'))