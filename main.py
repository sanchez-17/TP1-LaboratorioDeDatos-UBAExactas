# -*- coding: utf-8 -*-
"""
TP1LaboDatos.ipynb

Grupo: AltaData

Integrantes:Gaston Sanchez, Mariano Papaleo, Juan Pablo Hugo Aquilante

#Limpieza de datos/partición de (algunas) tablas

"""
#Carga de modulos y dataframes

import pandas as pd
from funciones import *
padron = './TablasOriginales/padron-de-operadores-organicos-certificados.csv'
salario = './TablasOriginales/w_median_depto_priv_clae2.csv'
loccensales = './TablasOriginales/localidades-censales.csv'
dicdepto = './TablasOriginales/diccionario_cod_depto.csv'
dicclase = './TablasOriginales/diccionario_clae2.csv'

df1 = pd.read_csv(padron,encoding = 'windows-1258')
df2 = pd.read_csv(salario)
df3 = pd.read_csv(loccensales)
df4 = pd.read_csv(dicdepto)
df5 = pd.read_csv(dicclase)

#%%
# DROPS DE LOS DATAFRAMES POR SER CONSIDERADOS COLUMNAS IRRELEVANTES O REDUNDANTES

df1 = df1.drop(['pais','pais_id','localidad'],axis=1)

df3 = df3.drop(60,axis=0).reset_index(drop=True)

#%%

# RENOMBRAMIENTO DE COLUMNAS, PONER EN MAYUSCULA STRINGS, QUITAR ACENTOS DE LOS STRINGS

sin_definir="INDEFINIDO" # para los NaNs

df1['departamento'] = df1['departamento'].str.upper()
df1['departamento'] = df1['departamento'].apply(quitar_acentos)

df2.rename(columns={'id_provincia_indec': 'provincia_id'}, inplace=True)
df2.rename(columns={'codigo_departamento_indec': 'departamento_id'}, inplace=True)

df3 = df3.rename(columns ={'provincia_nombre':'provincia'})
df3 = df3.rename(columns ={'departamento_nombre':'departamento'})
df3 = df3.rename(columns ={'municipio_nombre':'municipio'})
df3['departamento'] = df3['departamento'].str.upper()
df3['provincia'] = df3['provincia'].str.upper()
df3['nombre'] = df3['nombre'].str.upper()
df3.loc[df3.funcion.isna(),"funcion"]='SIN FUNCIÓN'
df3.loc[df3.municipio.isna(),"municipio"]=sin_definir
df3.loc[df3.municipio_id.isna(),"municipio_id"]=-99
df3.loc[df3.departamento_id.isna(),"departamento_id"]=-99
df3['departamento'] = df3['departamento'].apply(quitar_acentos)
df3['nombre'] = df3['nombre'].apply(quitar_acentos)

df4  = df4.rename(columns ={'codigo_departamento_indec': 'departamento_id'})
df4 = df4.rename(columns ={'nombre_departamento_indec':'departamento'})
df4  = df4.rename(columns ={'id_provincia_indec': 'provincia_id'})
df4 = df4.rename(columns ={'nombre_provincia_indec':'provincia'})
df4.loc[df4.provincia=="Tierra Del Fuego","provincia"]="TIERRA DEL FUEGO"
df4.loc[df4.provincia=="CABA","provincia"]="CIUDAD AUTONOMA BUENOS AIRES"
df4.loc[df4.departamento=="CABA","departamento"]="CIUDAD AUTONOMA BUENOS AIRES"
df4['departamento'] = df4['departamento'].str.upper()
df4['departamento'] = df4['departamento'].apply(quitar_acentos)
df4['provincia'] = df4['provincia'].str.upper()
df4['provincia'] = df4['provincia'].apply(quitar_acentos)

#%%

# Cosas a agregar para tener IDs en el df1 o para utilizar como diccionario más adelante

df1['id_operador'] = df1.index.values + 1 
df1.insert(0,'id_operador',df1.pop('id_operador'))


#%%

""" DF4 : DICCIONARIO DE DEPARTAMENTOS """

""" Añadimos a df4 ciertas IDs para cuando dada una provincia y un nombre de departamento, no existe un departamento_ID
que lo identifique entonces existe un ID que integra a estos no conocidos para cada provincia """

df4 = df4.append([{'departamento_id': 1002, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 2, 'provincia': 'CIUDAD AUTONOMA BUENOS AIRES'},
                  {'departamento_id': 1006, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 6, 'provincia': 'BUENOS AIRES'},
                  {'departamento_id': 1010, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 10, 'provincia': 'CATAMARCA'},
                  {'departamento_id': 1014, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 14, 'provincia': 'CORDOBA'},
                  {'departamento_id': 1018, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 18, 'provincia': 'CORRIENTES'},
                  {'departamento_id': 1022, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 22, 'provincia': 'CHACO'},
                  {'departamento_id': 1026, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 26, 'provincia': 'CHUBUT'},
                  {'departamento_id': 1030, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 30, 'provincia': 'ENTRE RIOS'},
                  {'departamento_id': 1034, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 34, 'provincia': 'FORMOSA'},
                  {'departamento_id': 1038, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 38, 'provincia': 'JUJUY'},
                  {'departamento_id': 1042, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 42, 'provincia': 'LA PAMPA'},
                  {'departamento_id': 1046, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 46, 'provincia': 'LA RIOJA'},
                  {'departamento_id': 1050, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 50, 'provincia': 'MENDOZA'},
                  {'departamento_id': 1054, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 54, 'provincia': 'MISIONES'},
                  {'departamento_id': 1058, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 58, 'provincia': 'NEUQUEN'},
                  {'departamento_id': 1062, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 62, 'provincia': 'RIO NEGRO'},
                  {'departamento_id': 1066, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 66, 'provincia': 'SALTA'},
                  {'departamento_id': 1070, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 70, 'provincia': 'SAN JUAN'},
                  {'departamento_id': 1074, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 74, 'provincia': 'SAN LUIS'},
                  {'departamento_id': 1078, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 78, 'provincia': 'SANTA CRUZ'},
                  {'departamento_id': 1082, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 82, 'provincia': 'SANTA FE'},
                  {'departamento_id': 1086, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 86, 'provincia': 'SANTIAGO DEL ESTERO'},
                  {'departamento_id': 1090, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 90, 'provincia': 'TUCUMAN'},
                  {'departamento_id': 1094, 'departamento': 'DEPARTAMENTO_DESCONOCIDO', 'provincia_id': 94, 'provincia': 'TIERRA DEL FUEGO'}],
                  ignore_index=True)

"""Partimos en sub-dataframes: """

""" DICCIONARIO DEPARTAMENTO"""

df4_departamento = df4[['departamento_id', 'departamento','provincia_id']].drop_duplicates().reset_index(drop=True)

""" DICCIONARIO PROVINCIA """

#correcciones para que df4 sea igual a df3
df4_provincia = df4[['provincia_id','provincia']].drop_duplicates().reset_index(drop =True)

"""Armamos el diccionario de DF4"""

# df4_dict es la versión normalizada de df4 que se conecta mediante la PK con df4_departamento y df4_provincia
df4_limpio = df4.drop('departamento',axis=1)
df4_limpio = df4_limpio.drop('provincia',axis=1)

#%%
""" DF3 : LISTADO DE LOCALIDADES CENSALES SEGÚN EL INDEC """

"""
Como la consigna del tp nos indica:

     "Esta fuente permite asociar a la fuente primaria “Padrón de Operadores Orgánicos
Certificados” con los datos de departamento. Lamentablemente la fuente primaria,
en su campo departamento parece mezclar datos de departamento y ciudad, entre
otras cosas. Esa fuente también tiene inconvenientes en cuanto al formato y
escritura de los nombres (por ejemplo, no parecen contar con tildes, etc.). Deberán
hacer lo necesario para curar y vincular los datos."

Por lo cual debemos vincular ambos dataframe por medio de los nombres de departamento 
e incluir en el df1 los departamento_id correspondientes a cada departamento que aparecen en df3.
"""

# Partimos en diccionarios para cada ID, para provincia_id, municipio_id, departamento_id

df3_provincia = df3[['provincia_id','provincia']].drop_duplicates().reset_index(drop =True)

df3_provincia=df3_provincia.sort_values(by=['provincia_id']) 

df3_municipio = df3[['municipio_id', 'municipio']].drop_duplicates().reset_index(drop=True)

df3_departamento = df3[['departamento_id', 'departamento']].drop_duplicates().reset_index(drop=True)

df3_localidad = df3[['id', 'nombre','funcion','centroide_lat','centroide_lon','categoria','fuente']].drop_duplicates().reset_index(drop=True)


df3_limpio = df3.drop('provincia',axis=1)
df3_limpio = df3_limpio.drop('municipio',axis=1)
df3_limpio = df3_limpio.drop('departamento',axis=1)
df3_limpio = df3_limpio.drop(labels=['nombre','funcion','centroide_lat','centroide_lon','categoria','fuente'],axis=1)

"""Ahora vinculariamos los df pero nos damos cuenta al hacerlo que hay varios departamentos con el mismo nombre 
pero en distintas provincias. Como ejemplo, tenemos GENERAL ROCA que esta tanto en RÍO NEGRO como en CÓRDOBA.
Por eso también tenemos que tener en cuenta las provincias_id a la hora de vincular los df
"""

df1['nombre'] = df1['departamento']
# Para el MERGE

df1.loc[df1['departamento'].str.contains('CAPITAL'),'departamento'] = 'CAPITAL'
# Cambia SALTA CAPITAL -> CAPITAL, ya que CAPITAL en la provincia de SALTA tiene un departamento_ID

# MERGE (LEFT JOIN) para añadir Departamento_IDs al primer df.

""" 1 ° MERGE. Primero merge con los nombres de departamento del df3.

2 ° MERGE. Luego merge con los nombres de localidad del df3,
ya que el df1 tiene nombres de localidad en la columna departamento en algunas entradas.

3 ° MERGE. Y por último merge con la df4 que es el diccionario de departamentos. """

df1= df1.merge(df3[['provincia_id','departamento','departamento_id']].drop_duplicates() , on=['provincia_id','departamento'], how='left')
df1= df1.merge(df3[['provincia_id','nombre','departamento_id']].drop_duplicates() , on=['provincia_id','nombre'], how='left',suffixes=['_1','_2'])
df1= df1.merge(df4[['provincia_id','departamento','departamento_id']].drop_duplicates() , on=['provincia_id','departamento'], how='left')
df1['departamento_combinado'] = df1['departamento_id_1'].combine_first(df1['departamento_id_2'])
df1['departamento_id_final'] = df1['departamento_combinado'].combine_first(df1['departamento_id'])


df1 = df1.drop(['nombre','departamento_id_1','departamento_id_2','departamento_id','departamento_combinado'],axis=1)
df1 = df1.rename(columns ={'departamento_id_final':'departamento_id'})
df1.insert(3,'departamento_id',df1.pop('departamento_id'))

print('Cantidad de NaNs en la columna departamento_id del df1(Padron):', len(df1.loc[df1.departamento_id.isna(),'departamento_id']))

#%%
""" ESTOS 124 NaNs corresponden a departamentos como OPEN DOOR, los cuales no aparecen en ningun df, 
 ni en el DF 4, ni en el DF 3 (ni como departamento, ni localidad).
 Por lo tanto, decimos asignarles un ID generico que creamos nosotros mismos. El ID generico es para departamentos 
 desconocidos de cada provincia. """

dptos_desc = df4.loc[df4.departamento == 'DEPARTAMENTO_DESCONOCIDO',['departamento_id','departamento','provincia_id']]
df1= df1.merge(dptos_desc[['provincia_id','departamento_id']], on=['provincia_id'], how='left', suffixes = ['_1','_2'])
df1['departamento_id'] = df1['departamento_id_1'].combine_first(df1['departamento_id_2'])
df1 = df1.drop(['departamento_id_1','departamento_id_2'],axis=1)
df1.insert(3,'departamento_id',df1.pop('departamento_id'))

print('Cantidad de NaNs en la columna departamento_id del df1(Padron):', len(df1.loc[df1.departamento_id.isna(),'departamento_id']))

#Exito

#%%

""" Por último chequeamos que cada departamento_id de df1 este en el diccionario de departamento """

df1[~df1.departamento_id.isin(df4.departamento_id)] 

""" Hay 3 entradas que tienen id 94008 que son de PROVINCIA: TIERRA DEL FUEGO, DEPARTAMENTO : RIO GRANDE 

Dado que el DataFrame 2 de salarios privados tiene la ID de RIO GRANDE como 94007, y la de USHUAIA COMO 94014. 
Osea que nuestro diccionario de departamentos esta bien 
Pero el ID esta mal puesto en df1 asi que lo cambiamos a mano. (Solo cambiamos 94008 ya que no hay 94015 )"""

df1.loc[[970,971,972], "departamento_id"] = 94007

#%%

print('Cantidad de id_departamentos que no estan en el diccionario de departamentos: ',
      len(df1[~df1.departamento_id.isin(df4.departamento_id)])) 

# Perfect


#%%
""" DF2 : SALARIOS PROMEDIOS PRIVADOS """

""" Como saber cuantos registros de la columna 'w_median' son inferiores a 0 """

df2_salarios_negativos = df2.loc[df2['w_median'] < 0] 


""" ¿Cuánto representa esa cantidad respecto del total? """

len(df2_salarios_negativos)/len(df2)*100 # 22,42 %

#Luego para ubicar los valores NaN en la tabla

NaN_filas = df2[df2.isna().any(axis=1)] # Con esto sabemos cuantas filas tienen al menos 1 NaN #9156
NaN_columnas = df2.columns[df2.isna().any()].tolist() # Con esto sabemos que columnas tienen NaN

# Y ahora podemos fijarnos si los NaN aparecen simultaneamente en ambas columnas o si se dividen apareciendo a veces
# en una y a veces en la otra.

dptoNaN = df2['departamento_id'].isna() # Retorna 9156
provNaN = df2['provincia_id'].isna() # Retorna 9156

# Pero con esto no nos alcanza asi que por último chequeamos si cada vez que aparece False o True en dptoNaN 
# se corresponde con los False o True de provNaN. Eso daria una serie de Pandas llena de valores True. Y si el sum()
# (que solo cuenta los True) es igual a la len del dataframe original entonces cada vez que aparece NaN en una aparece
# en la otra.

(dptoNaN == provNaN).sum() == len(df2)  # Esto da True.

df2 = df2.dropna() 
# Armamos otro dataframe sin los NaNs ya que solo son 9156

# Pasamos la fecha a mes y anio (año)

df2['fecha'] = pd.to_datetime(df2['fecha'])
df2['anio'] = df2['fecha'].dt.year
df2['mes'] = df2['fecha'].dt.month

df2 = df2.drop('fecha',axis=1)

df2.insert(0,'anio',df2.pop('anio'))
df2.insert(1,'mes',df2.pop('mes'))

# Dropeamos la fecha original del dataframe y solo dejamos el mes y anio
# Los posicionamos en la columna 0 y 1

#%%
""" DF 5 : DICCIONARIO DE CLASES """

""" El DF 5 tiene un valor NaN cuando 'clae2' es igual a 999. 
Esto es debido a que el valor 'OTROS' no tiene asignada una letra. Por lo que decidimos asignarle la Z. """

df5.loc[85,'letra'] = 'Z'

"""Dividimos en sub-dataframes: """

""" DICCIONARIO CLAE2"""

df5_clae2 = df5[['clae2', 'clae2_desc']].drop_duplicates().reset_index(drop=True)

""" DICCIONARIO LETRA"""

df5_letra = df5[['letra', 'letra_desc']].drop_duplicates().reset_index(drop=True)

""" DF 5 LIMPIO """

df5_limpio = df5.drop('clae2_desc',axis=1)
df5_limpio = df5_limpio.drop('letra_desc',axis=1)

""" es dificil normalizar df5_clae2 ya que las descripciones estan pensadas 
 como string y no para separarse en valores atomizados. """

## atomizarColumna(df5_clae2,'clae2_desc', ', ')
## atomizarColumna(df5_clae2,'clae2_desc',' y ')


"""NORMALIZAMOS df5_letra 

# Para ello hace falta renombrar gran parte de las descripciones ya que tienen errores de ortografía 
 o cosas como puntos y coma/dos puntos en lugares donde no van. """


df5_letra = df5_letra.replace({'EXPLOTACION DE MINAS Y CANTERAS' : 'EXPLOTACIÓN DE MINAS Y EXPLOTACION DE CANTERAS',
                               ' SUMINISTRO DE ELECTRICIDAD, GAS, VAPOR Y AIRE ACONDICIONADO' : 'SUMINISTRO DE ELECTRICIDAD,SUMINISTRO DE GAS, SUMINISTRO DE VAPOR, SUMINISTRO DE AIRE ACONDICIONADO',
                            ' SUMINISTRO DE AGUA; CLOACAS; GESTIÓN DE RESIDUOS Y RECUPERACIÓN DE MATERIALES Y SANEAMIENTO PUBLICO':'SUMINISTRO DE AGUA, CLOACAS, GESTION DE RESIDUOS, RECUPERACIÓN DE MATERIALES, SANEAMIENTO PUBLICO',
                        	' COMERCIO AL POR MAYOR Y AL POR MENOR; REPARACIÓN DE VEHÍCULOS AUTOMOTORES Y MOTOCICLETAS' :'COMERCIO AL POR MAYOR, COMERCIO AL POR MENOR, REPARACION DE VEHICULOS AUTOMOTORES, REPARACION DE MOTOCICLETAS',
                        	' SERVICIO DE TRANSPORTE Y ALMACENAMIENTO':'SERVICIO DE TRANSPORTE, SERVICIO DE ALMACENAMIENTO',
                        	' SERVICIOS PROFESIONALES, CIENTÍFICOS Y TÉCNICOS': 'SERVICIOS PROFESIONALES, SERVICIOS CIENTIFICOS, SERVICIOS TECNICOS',
                        	' SERVICIOS  ARTÍSTICOS, CULTURALES, DEPORTIVOS  Y DE ESPARCIMIENTO': 'SERVICIOS ARTISTICOS, SERVICIOS CULTURALES, SERVICIOS DEPORTIVOS, SERVICIOS DE ESPARCIMIENTO',
                            })

#atomizarColumna(df5_letra,'letra_desc', ', ')
#atomizarColumna(df5_letra,'letra_desc',' y ')
    

df5_letra.letra_desc = df5_letra.letra_desc.apply(sacar_espacios_en_extremos) 
# esta función elimina los espacios al principio y final de las palabras gracias a la función strip

#%%
""" DF 1 : PADRON """

"--------------Tratamiento de NaNs-------------------"

#Veamos si para un producto."CAMPO INCULTO" aplica un valor en especifico para 
#la columna rubro
#Filtramos nans con mascara para evitar mensaje de error:
mask = ~df1.rubro.isna()
df1_rubro_productos_sinNan = df1.loc[df1.productos.str.contains("INCULTO") & mask,["productos","rubro"]]
#Vemos que INCULTO refiere al rubro AGRICULTURA, redefinimos los nan
df1_producto_inculto_conNan = df1.loc[df1.rubro.isna(),["rubro","productos"]]
df1.at[854, 'rubro'] = sin_definir
df1.at[880, 'rubro'] = sin_definir
df1.at[909, 'rubro'] = sin_definir
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

# Corregimos algunas cosas a mano

con_campo_monte_o_pasturas = df1.productos.str.contains("CAMPO") | df1.productos.str.contains("MONTE") | df1.productos.str.contains("PASTURAS")
#Definimos el producto como "INCULTO" y rubro "AGRICULTURA"
df1.loc[con_campo_monte_o_pasturas,"productos"] = "INCULTO"
df1.loc[con_campo_monte_o_pasturas,'rubro'] = "AGRICULTURA"

#%%
"""-------------------------------PRODUCTOS--------------------------------------------"""

df1_productos = df1.copy()
df1_productos = df1_productos[['id_operador','productos']]

prod_con_parentesis = df1_productos.loc[df1_productos.productos.str.contains("\("),"productos"].unique()
#hay 1 valor con "HORTICULTURA: (RAIZ, HOJAS, FRUTOS) - FRUTALES: (CAROZO, PEPITA, CITRI ..."
con_comas_en_parentesis = df1_productos.productos.str.contains("RAIZ, HOJAS, FRUTOS")
vista = df1_productos.loc[con_comas_en_parentesis,:]
##Lo modificamos manualmente para que rubro contemple a FRUTICULTURA, y productos no tenga parentesis
#crear_y_añadir_fila(df1_productos,124,"rubro","HORTICULTURA")
##nos falta modificar su columna productos a esta fila creada
#df1_productos.at[len(df1_productos)-1, "productos"] = "RAIZ,HOJAS,FRUTOS"
#
#crear_y_añadir_fila(df1_productos,124,"rubro","FRUTICULTURA")
##nos falta midificar su columna productos a esta fila creada
#df1_productos.at[len(df1_productos)-1, "productos"] = "CAROZO,PEPITA,CITRICOS"
##Borramos la fila 124
#df1_productos.drop([124],inplace=True)
#df1_productos.reset_index(drop=True, inplace=True)
#hay 3 valores con "(CARNE Y LANA)"
con_carne_lana = df1_productos.productos.str.contains("CARNE Y LANA")
df1_productos.loc[con_carne_lana,"productos"] = "CARNE BOVINA,CARNE OVINA,LANA"
df1_productos.productos = df1_productos.productos.apply(quitar_parentesis)

#%%
#Spliteamos por comas
col = "productos"
atomizarColumna(df1_productos,col,',')
#verificamos si siguen habiendo parentesis
print('Cantidad de comas en df1_productos :' , len(df1.loc[df1_productos.productos.str.contains("\("),"productos"])) #0

#%%

#Se decide que lana corresponde al rubro procesamiento textil y los distintos tipos de lanas se unifican en un solo producto: "lana". 
# Excepto 3 en donde sus valores son "TOPS DE LANA" o "BLOUSSE" que quedan en sus respectivos rubros
contienen_lana = df1_productos.productos.str.contains("LANA") &  ~df1_productos.productos.str.contains("AVELLANA") & ~df1_productos.productos.str.contains("BLOUSSE") & ~df1_productos.productos.str.contains("TOPS DE LANA")
df1_productos.loc[contienen_lana,"productos"] = "LANA"

contienen_lana = df1.rubro.str.contains('INDEFINIDO') & df1.productos.str.contains("LANA") &  ~df1.productos.str.contains("AVELLANA") & ~df1_productos.productos.str.contains("BLOUSSE") & ~df1_productos.productos.str.contains("TOPS DE LANA")
df1.loc[contienen_lana,'rubro'] = "PROCESAMIENTO TEXTIL"

#verifico
df1_productos.loc[contienen_lana,"productos"]
df1.loc[contienen_lana,"rubro"]

#%%
"""-------------------------------Atomizamos df1_productos--------------------------------------------"""

#Quitamos puntos

df1_productos.productos = df1_productos.productos.apply(reemplazar,args=(".",""))

#%%

# Puntos y comas
atomizarColumna(df1_productos,col,';')
#%%
# Guiones
atomizarColumna(df1_productos,col,'-')
#%%
# Simbolos MÁS
atomizarColumna(df1_productos,col,'+')
#%%
# Signos de Interrogación
atomizarColumna(df1_productos,col,'?')
#%%
# Ygriegas
atomizarColumna(df1_productos,col,' Y ')
#%%

#Limpiamos los espacios en blanco

df1_productos.productos = df1_productos.productos.apply(sacar_espacios_en_extremos)

#%%
##Es posible que tengamos problemas al separar por " Y ". La tarea no es trivial.  VERIFICAMOS
msk = df1_productos.productos.str.contains(" Y ")
b = df1_productos.loc[msk,:]
print(len(b))

# Perfecto

#%%
"""-------------------------------RUBRO--------------------------------------------"""

#Lo modificamos manualmente para que rubro contemple a FRUTICULTURA
df1.loc[124,"rubro"] = "HORTICULTURA,FRUTICULTURA"

#Para todos aquellos productos que tengan "INCULTO", definir en columna rubro AGRICULTURA
filtro = df1.productos.str.contains("INCULTO")
df1.loc[filtro,'rubro'] = "AGRICULTURA"

#Todos los productos que contengan la palabra campo, natural reemplazar el valor por inculto
filtro = df1.productos.str.contains("CAMPO") | df1.productos.str.contains("MONTE") | df1.productos.str.contains("PASTURAS")
df1.loc[filtro,"rubro"] = "INCULTO"

#Aquellos que tienen categoria "Comercializadores" y no tienen rubro, le definiremos como "VENTAS"
df1.loc[df1.rubro.str.contains("INDEFINIDO") & df1.categoria_desc.str.contains("Comercializadores"),"rubro"] = "VENTAS"

# Corregimos 3 casos a mano en donde dice"--------------Tratamiento de NaNs-------------------" 'AGICULTURA'

df1.at[553, 'rubro'] = 'AGRICULTURA/FRUTICULTURA'
df1.at[144, 'rubro'] = 'AGRICULTURA/HORTICULTURA'
df1.at[549, 'rubro'] = 'AGRICULTURA/HORTICULTURA'

#Veamos si hay puntos
print('Cantidad de Puntos en df1.rubro:',len(df1.loc[df1.rubro.str.contains("\."),"rubro"]))
df1.rubro = df1.rubro.apply(reemplazar,args=(".",","))
print('Cantidad de Puntos en df1.rubro luego de limpiar:',len(df1.loc[df1.rubro.str.contains("\."),"rubro"]))
print('Cantidad de Puntos y coma en df1.rubro:',len(df1.loc[df1.rubro.str.contains("\."),"rubro"]))
df1.rubro = df1.rubro.apply(reemplazar,args=(";",","))
print('Cantidad de Puntos y coma en df1.rubro luego de limpiar:',len(df1.loc[df1.rubro.str.contains("\."),"rubro"]))

#%%
# Espacios en blanco en rubro

df1.rubro = df1.rubro.apply(sacar_espacios_en_extremos)

#%%
"""-------------------------------ESTABLECIMIENTO--------------------------------------------"""
df1.loc[df1.establecimiento == "NC","establecimiento"] = sin_definir
df1.to_csv('./TablasLimpias/df1.csv', index=False)

#%%
"""
"--------------AÑADIENDO LAS CLASES A LOS OPERADORES------------------"
Añadimos una Clae2 a cada operador para poder relacionarlo con el df1 con el df2 y poder asociarles a futuro 
un salario promedio según la actividad y el departamento de cada operador.

"""

rubros_unicos = df1['rubro'].drop_duplicates()

claves = rubros_unicos.copy().reset_index(drop=True)
valores = pd.Series([1,1,1,1,1,1,1,1,2,1,999,1,1,3,999,10,10,10,52,10,10,10,10,10,10,999,10,10,10,10,
                     10,10,21,10,10,10,10,10,10,52,11,10,10,10,10,13,10,10,10,10,11,11,11,10,10,10,11,
                     10,52,11,10,11,10,10,11,10,10,52,10,10,10,10,11,10,10,10,10,52,10,10,10,10,11,
                     11,11,10,11,10,10,10,10,10,10,10,10,999])

claves_y_valores = pd.DataFrame({'claves':claves, 'valores' : valores})
# Podemos ver que clae2 tiene cada rubro en un dataframe

diccionario_clase = {}

for clave, valor in zip(claves, valores):
    diccionario_clase[clave] = valor
    

# Decidimos asignarle clae2 = 999 a los que tienen rubro INDEFINIDO

df1["clae2"] = df1["rubro"].apply(lambda rubro: diccionario_clase.get(rubro))
df1 = df1.astype({"clae2": int})

# Agregamos una columna clae2 en el df1 que indique su clase.

print('Cantidad de NaNs en la columna clae2 que recien agregamos:',len(df1[df1.clae2.isna()]))

#%%
""" DICCIONARIOS DE ID_CERTIFICADORA Y ID_CATEGORIA """

# Armamos un tabla que sirva de diccionario de id_certificadora
df1_certificadora = df1[['Certificadora_id','certificadora_deno']].drop_duplicates().reset_index(drop = True)
df1_certificadora=df1_certificadora.sort_values(by=['Certificadora_id']) 
certificadora = df1_certificadora.rename(columns ={'Certificadora_id' : 'id_certificadora'})


# Armamos un tabla que sirva de diccionario de id_categoria
df1_categoria = df1[['categoria_id','categoria_desc']].drop_duplicates().reset_index(drop = True)
df1_categoria=df1_categoria.sort_values(by=['categoria_id'])
categoria = df1_categoria.rename(columns ={'categoria_id' : 'id_categoria'})

#%%

""" Antes de exportar las tablas, hacemos algunos renames, drops e inserts
para que los nombres de las tablas coincidan con el MER """

#Drops
df1 = df1.drop(['provincia_id','provincia','departamento','productos','categoria_desc','certificadora_deno'],axis=1)

#Inserts
df1.insert(2,'clae2',df1.pop('clae2'))

#Renames
df1 = df1.rename(columns ={'departamento_id':'id_departamento', 'clae2' : 'id_clase','categoria_id' : 'id_categoria',
                           'razón social' : 'razon_social', 'Certificadora_id' : 'id_certificadora',})
    
df2 = df2.rename(columns ={'departamento_id':'id_departamento',"provincia_id":"id_provincia","clae2":"id_clase","w_median":"salario_promedio"})

df3 = df3.rename(columns ={'municipio_nombre':'municipio'})

df4_departamento = df4_departamento.rename(columns ={'departamento_id':'id_departamento', 'departamento' : 'nombre_departamento'
                                                     ,'provincia_id' : 'id_provincia'})
    
df4_provincia = df4_provincia.rename(columns ={'provincia_id' : 'id_provincia','provincia' : 'nombre_provincia'})

df5_clae2 = df5_clae2.rename(columns ={'clae2':'id_clase','clae2_desc' : 'descripcion'})
df4_provincia.at[0,'nombre_provincia'] = 'CABA'

#%%%

"""--------------Exportamos todos los csv a la carpeta TablasLimpias------------------"""


df1.to_csv('./TablasLimpias/operador.csv', index=False)
df1_productos.to_csv('./TablasLimpias/producto.csv', index=False)
df5_clae2.to_csv('./TablasLimpias/clase.csv', index=False)
categoria.to_csv('./TablasLimpias/categoria.csv', index=False)
df4_departamento.to_csv('./TablasLimpias/departamento.csv', index=False)
df4_provincia.to_csv('./TablasLimpias/provincia.csv', index=False)
certificadora.to_csv('./TablasLimpias/certificadora.csv', index=False)
df2.to_csv('./TablasLimpias/salario.csv', index=False)

df3_limpio.to_csv('./TablasLimpias/df3_limpio.csv', index=False)
df4_limpio.to_csv('./TablasLimpias/df4_limpio.csv', index=False)
df5_limpio.to_csv('./TablasLimpias/df5.csv', index=False)
df5_letra.to_csv('./TablasLimpias/df5_letras.csv', index=False)

