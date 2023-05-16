#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import re 
from unidecode import unidecode


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

def sacar_espacios_columna(df,columna):
    df[columna] = df[columna].str.strip()
    
def quitar_acentos(palabra):
    return unidecode(palabra)

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
prod_con_parentesis = df1.loc[df1.productos.str.contains("\("),"productos"].unique()
#hay 1 valor con "HORTICULTURA: (RAIZ, HOJAS, FRUTOS) - FRUTALES: (CAROZO, PEPITA, CITRI ..."
con_comas_en_parentesis = df1.productos.str.contains("RAIZ, HOJAS, FRUTOS")
vista = df1.loc[con_comas_en_parentesis,:]
#Lo modificamos manualmente para que rubro contemple a FRUTICULTURA, y productos no tenga parentesis
crear_y_añadir_fila(df1,124,"rubro","HORTICULTURA")
#nos falta modificar su columna productos a esta fila creada
df1.at[len(df1)-1, "productos"] = "RAIZ,HOJAS,FRUTOS"

crear_y_añadir_fila(df1,124,"rubro","FRUTICULTURA")
#nos falta midificar su columna productos a esta fila creada
df1.at[len(df1)-1, "productos"] = "CAROZO,PEPITA,CITRICOS"
#Borramos la fila 124
df1.drop([124],inplace=True)
df1.reset_index(drop=True, inplace=True)
#hay 3 valores con "(CARNE Y LANA)"
con_carne_lana = df1.productos.str.contains("CARNE Y LANA")
df1.loc[con_carne_lana,"productos"] = "CARNE BOVINA,CARNE OVINA,LANA"

col = "productos"
df1.productos = df1.productos.apply(quitar_parentesis)
#Spliteamos por comas
atomizarColumna(df1,col,',')
#verificamos si siguen habiendo parentesis
a=df1.loc[df1.productos.str.contains("\("),"productos"] #0

#Se decide que lana corresponde al rubro procesamiento textil y los distintos tipos de lanas se unifican en un solo producto: "lana". Excepto 3 en donde sus valores son "TOPS DE LANA" o "BLOUSSE" que quedan en sus respectivos rubros
contienen_lana = df1.productos.str.contains("LANA") &  ~df1.productos.str.contains("AVELLANA") & ~df1.productos.str.contains("BLOUSSE") & ~df1.productos.str.contains("TOPS DE LANA")
df1.loc[contienen_lana,["rubro","productos"]] = ["PROCESAMIENTO TEXTIL","LANA"]
#verifico
df1.loc[contienen_lana,["rubro","productos"]]
a = df1.productos.unique()
#%%
#Lo modificamos manualmente para que rubro contemple a FRUTICULTURA
df1.loc[124,"rubro"] = "HORTICULTURA,HORTICULTURA"
#%%
atomizarColumna(df1,col,';')
atomizarColumna(df1,col,'-')
atomizarColumna(df1,col,'+')
atomizarColumna(df1,col,'?')
#%%
#Quitamos puntos
df1.productos = df1.productos.apply(reemplazar,args=(".",""))

con_campo_monte_o_pasturas = df1.productos.str.contains("CAMPO") | df1.productos.str.contains("MONTE") | df1.productos.str.contains("PASTURAS")
#Definimos el producto como "INCULTO" y rubro "AGRICULTURA"
df1.loc[con_campo_monte_o_pasturas,"productos"] = "INCULTO"
df1.loc[con_campo_monte_o_pasturas,'rubro'] = "AGRICULTURA"

#%%
#Algunos registros se cambian manualmente, pues si atomizamos por " y " daran problemas de calidad
con_y = df1.productos.str.contains(" Y ")
ver_registros= df1.loc[con_y,["rubro","productos"]]
df1.loc[2775,["rubro","productos"]] = ["ELABORACION","PULPA DE MANZANA Y JUGO DE MANZANA Y MANZANA DESHIDRATADA"]
df1.loc[3200,["rubro","productos"]] = ["PROCESAMIENTO","CEREALES Y OLEAGINOSAS"]
df1.loc[2880,"productos"] = "JUGO DE LIMON Y ACEITE DE LIMON"
df1.loc[3254,["rubro","productos"]] = ["ALMACENAMIENTO Y FRIO","JUGO CONCENTRADO DE PERA Y PURE DE PERA"]
df1.loc[2781,["rubro","productos"]] = ["ALMACENAMIENTO Y FRIO","JUGO CONCENTRADO DE PERA Y JUGO CONCENTRADO DE MANZANA"]
df1.loc[2246,["rubro","productos"]] = ["FRUTICULTURA-HORTICULTURA","FRUTAS Y HORTICULTURA"]
df1.loc[2219,"rubro"] = "FRUTICULTURA-HORTICULTURA"
df1.loc[3186,["rubro","productos"]] = ["VENTAS","MANI"]
df1.loc[3165,["rubro","productos"]] = ["VENTAS","CEREALES Y OLEAGINOSAS"]
df1.loc[1576,"productos"] = "CEREALES Y OLEAGINOSAS"
df1.loc[2768,["rubro","productos"]] = ["ELABORACION","JUGO CONCENTRADO DE PERA Y JUGO CONCENTRADO DE MANZANA"]
df1.loc[2737,["rubro","productos"]] = ["ELABORACION","JUGO CONCENTRADO DE PERA Y JUGO CONCENTRADO DE MANZANA"]
df1.loc[2782,"rubro"] = "FRUTICULTURA"
df1.loc[2786,"rubro"] = "FRUTICULTURA"
df1.loc[2221,"rubro"] = "APICULTURA"
df1.loc[2878,"rubro"] = "APICULTURA"
df1.loc[2339,"rubro"] = "APICULTURA"
df1.loc[3166,"rubro"] = "APICULTURA"
df1.loc[3276,"rubro"] = "APICULTURA"
#Atomizamos
atomizarColumna(df1,col,' Y ')
#Limpiamos los espacios en blanco
df1.productos = df1.productos.apply(sacar_espacios_en_extremos)
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
filtro = df1.productos.str.contains("CAMPO") | df1.productos.str.contains("MONTE") | df1.productos.str.contains("PASTURAS")
df1.loc[filtro,"rubro"] = "INCULTO"
#%%
"""-------------------------------COLUMNA RUBRO--------------------------------------------"""

padron_productos_atom = './dataframes/checkpoints/padron_productos_atomizados.csv'
df1 = pd.read_csv(padron_productos_atom)

#Aquellos que tienen categoria "Comercializadores" y no tienen rubro, le definiremos como "VENTAS"
df1.loc[df1.rubro.str.contains("INDEFINIDO") & df1.categoria_desc.str.contains("Comercializadores"),"rubro"] = "VENTAS"
#Hay casos en donde esta el rubro elaboracion, pero al separar por comas se rompe. Lo corregimos manualmente
aux=df1.loc[df1.rubro.str.contains("ACEITE ESCENCIAL Y PULPA DE CITRICOS"),["rubro","productos"]]
df1.loc[df1.rubro.str.contains("ACEITE ESCENCIAL Y PULPA DE CITRICOS"),"rubro"] = "ELABORACION"
#Veamos para separar por comas
a=df1.loc[df1.rubro.str.contains(","),["rubro","productos"]]
#No hay casos particulares, separamos:
atomizarColumna(df1,"rubro",',')
#Veamos para separar por punto y coma
a=df1.loc[df1.rubro.str.contains(";"),["rubro","productos"]]
#No hay casos particulares, separamos:
atomizarColumna(df1,"rubro",';')
#Veamos para separar por guiones
a=df1.loc[df1.rubro.str.contains("-"),["rubro","productos"]]
#No hay casos particulares, separamos:
atomizarColumna(df1,"rubro",'-')
#Veamos para separar por barras
a=df1.loc[df1.rubro.str.contains("/"),["rubro","productos"]]
#No hay casos particulares, separamos:
atomizarColumna(df1,"rubro",'/')
#Luego de separar por barra, surgieron otra vez errrores de tipo
df1.loc[df1.rubro == "AGICULTURA","rubro"] = "AGRICULTURA"
#Veamos si hay puntos
a=df1.loc[df1.rubro.str.contains("\."),"rubro"]
atomizarColumna(df1,"rubro",'\.')

#Es posible que tengamos problemas al separar por " Y ". La tarea no es trivial.
a = df1.loc[df1.rubro.str.contains(" Y "),["rubro","productos"]]
#Caso en donde contiene la palabra "PROCESAMIENTO"
b = df1.loc[df1.rubro.str.contains(" Y ") & df1.rubro.str.contains("PROCESAMIENTO"),["rubro","productos"]]
#Hay un solo caso particular: "PROCESAMIENTO DE TE Y MOLINO YERBA MATE". Los demas solo pueden reemplazarse por la palabra "PROCESAMIENTO"
df1.loc[df1.rubro.str.contains("PROCESAMIENTO FRUTALES Y HORTALIZAS") |
        df1.rubro.str.contains("PROCESAMIENTO CEREALES Y OLEAGINOSAS") |
        df1.rubro.str.contains("PROCESAMIENTO DE CEREALES Y OLEAGINOSAS") |
        df1.rubro.str.contains("PROCESAMIENTO DE MANI Y SOJA")
        ,"rubro"] = "PROCESAMIENTO"
#Veamos mas casos
aux=df1.loc[df1.rubro.str.contains(" Y ") ,["rubro","productos"]].rubro.value_counts()
df1.loc[df1.rubro.str.contains("ACOPIO Y ACONDICIONAMIENTO DE GRANOS") ,"rubro",] = "ACOPIO-ACONDICIONAMIENTO"
df1.loc[df1.rubro.str.contains("FRACCIONAMIENTO Y DEPOSITO DE HIERBAS AROMATICAS Y MEDICINALES") ,"rubro",] = "FRACCIONAMIENTO-DEPOSITO"
df1.loc[df1.rubro.str.contains("ELABORACION DE  JUGOS CONCENTRADOS Y FABRICA DE ACEITES ESENCIALES") ,"rubro",] = "ELABORACION"
df1.loc[df1.rubro.str.contains("EMPAQUE Y FRIGORIFICO DE FRUTAS NO CITRICAS") ,"rubro",] = "EMPAQUE-FRIGORIFICO"
df1.loc[df1.rubro.str.contains("EMPAQUE Y FRIGORIFICO FRUTAS NO CITRICAS ") ,"rubro",] = "EMPAQUE-FRIGORIFICO"
df1.loc[df1.rubro.str.contains("ELABORACION DE JUGO CONCENTRADO DE MANZANA Y PERA") ,"rubro",] = "ELABORACION"
df1.loc[df1.rubro.str.contains("MOLINO Y FRACCIONAMIENTO DE TE") ,"rubro",] = "MOLINO-FRACCIONAMIENTO"
df1.loc[df1.rubro.str.contains("ELABORACION Y EXTRACCION DE ACEITE") ,"rubro",] = "ELABORACION-EXTRACCION"
df1.loc[df1.rubro.str.contains("BODEGA VITIVINICOLA Y ELABORACION DE  VINAGRE") ,"rubro",] = "BODEGA-ELABORACION"
df1.loc[df1.rubro.str.contains("EMPAQUE DE HORTALIZAS Y FRUTAS NO CITRICAS") ,"rubro",] = "EMPAQUE"
df1.loc[df1.rubro.str.contains("ELABORACION DE DULCES Y FRUTAS EN ALMIBAR") ,"rubro",] = "ELABORACION"
df1.loc[df1.rubro.str.contains("PROCESAMIENTO DE TE Y MOLINO YERBA MATE") ,"rubro",] = "PROCESAMIENTO-MOLINO"
df1.loc[df1.rubro.str.contains("ELABORACION DE MANZANA Y PERA DEHIDRATADA") ,"rubro",] = "ELABORACION"
df1.loc[df1.rubro.str.contains("ALMACENAMIENTO Y FRIO PARA FRUTAS NO CITRICAS") ,"rubro",] = "ALMACENAMIENTO-FRIO"
df1.loc[df1.rubro.str.contains("EXTRACCION DE ACEITE Y ELABORACION DE ACEITUNAS Y OTROS") ,"rubro",] = "EXTRACCION-ELABORACION"
df1.loc[df1.rubro.str.contains("PROCESADO Y ENVASADO DE HORTALIZAS") ,"rubro",] = "PROCESADO-ENVASADO"
df1.loc[df1.rubro.str.contains("ELABORACION DE JUGOS Y BODEGA VITIVINICOLA") ,"rubro",] = "ELABORACION-BODEGA"
df1.loc[df1.rubro.str.contains("FRACCIONAMIENTO Y EMPAQUE DE ARROZ") ,"rubro",] = "FRACCIONAMIENTO-EMPAQUE"
df1.loc[df1.rubro.str.contains("FRIGORIFICOS Y EMPAQUE PARA  FRUTAS") ,"rubro",] = "FRIGORIFICOS-EMPAQUE"
#Definimo el rubro "APICULTURA" y productos "MIEL"
aux = df1.loc[df1.rubro.str.contains("EXTRACCION Y FRACCIONAMIENTO DE MIEL"),["rubro","productos"]]
df1.loc[df1.rubro.str.contains("EXTRACCION Y FRACCIONAMIENTO DE MIEL"),["rubro","productos"]] = ["APICULTURA","MIEL"]
#Solo falta separar por " Y ", "-". Finalmente quitar espacios en blanco extremos
atomizarColumna(df1,"rubro",' Y ')
atomizarColumna(df1,"rubro",'-')
df1.rubro = df1.rubro.apply(sacar_espacios_en_extremos)
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

# PARTIENDO EL DATAFRAME 4 

# PRIMERO RENOMBRAMOS 

df4  = df4.rename(columns ={'codigo_departamento_indec': 'codigo_departamento'})
df4 = df4.rename(columns ={'nombre_departamento_indec':'nombre_departamento'})
df4  = df4.rename(columns ={'id_provincia_indec': 'provincia_id'})
df4 = df4.rename(columns ={'nombre_provincia_indec':'nombre_provincia'})

#PARTIMOS DEPARTAMENTO
df4_departamento = df4[['codigo_departamento', 'nombre_departamento']].drop_duplicates().reset_index(drop=True)

#PARTIMOS PROVINCIA

#correcciones para que sea igual a df3
df4.loc[df4.nombre_provincia=="Tierra Del Fuego","nombre_provincia"]="Tierra del Fuego, Antártida e Islas del Atlántico Sur"
df4.loc[df4.nombre_provincia=="CABA","nombre_provincia"]="Ciudad Autónoma de Buenos Aires"
df4_provincia = df4[['provincia_id','nombre_provincia']].drop_duplicates().reset_index(drop =True)

# df4_dict es la versión normalizada de df4 que se conecta mediante la PK con df4_departamento y df4_provincia
df4_dict = df4.drop('nombre_departamento',axis=1)
df4_dict = df4_dict.drop('nombre_provincia',axis=1)


#%% como dataframe/tabla de provincia se usa df4_provincia con las siguientes correcciones


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
"****************DF3********************"
#TRATAMIENTO DE NANS

df3.loc[df3.nombre_departamento.isna(),"nombre_departamento"]=sin_definir
df3.loc[df3.funcion.isna(),"funcion"]='SIN FUNCIÓN'
df3.loc[df3.nombre_municipio.isna(),"nombre_municipio"]=sin_definir
#asumimos que aquellos municipios sin id, sean -99 y representan a un valor numerico sin definir
df3.loc[df3.municipio_id.isna(),"municipio_id"]=-99
df3.loc[df3.departamento_id.isna(),"departamento_id"]=-99

#PARTIMOS PROVINCIA
df3_provincia = df3[['provincia_id','nombre_provincia']].drop_duplicates().reset_index(drop =True)
#correccion para que se asemeje a df4
df3_provincia=df3_provincia.sort_values(by=['provincia_id']) 
df3_dict = df3.drop('nombre_provincia',axis=1)

#PARTIMOS MUNICIPIO
df3_municipio = df3[['municipio_id', 'nombre_municipio']].drop_duplicates().reset_index(drop=True)
df3_dict = df3_dict.drop('nombre_municipio',axis=1)

#PARTIMOS DEPARTAMENTO
df3_departamento = df3[['departamento_id', 'nombre_departamento']].drop_duplicates().reset_index(drop=True)
df3_dict = df3_dict.drop('nombre_departamento',axis=1)

#PARTIMOS LOCALIDAD
df3_localidad = df3[['id', 'nombre','funcion','centroide_lat','centroide_lon','categoria','fuente']].drop_duplicates().reset_index(drop=True)
df3_dict = df3_dict.drop(labels=['nombre','funcion','centroide_lat','centroide_lon','categoria','fuente'],axis=1)
"""
Nos dimos cuenta que el df3 tiene las id de los departamentos que aparecen en el df1. 
Por lo cual debemos vincular ambos dataframe por medio de los nombres de departamento e incluir en el df1 
los id correspondientes a cada departamento que aparecen en df3.
"""
# PASOS

# Primero renombramos en el df3 las columnas para que matcheen con las del df1

df3 = df3.rename(columns ={'provincia_nombre':'provincia'})
df3 = df3.rename(columns ={'municipio_nombre':'municipo'})
df3 = df3.rename(columns ={'departamento_nombre':'departamento'})

# Segundo pasamos los nombres de los departamentos de ambos dataframe a mayuscula

df3['departamento'] = df3['departamento'].str.upper()
df1['departamento'] = df1['departamento'].str.upper()

"""
Ahora vinculariamos los df pero nos damos cuenta al hacerlo que hay varios departamentos con el mismo nombre 
pero en distintas provincias. Como ejemplo, tenemos GENERAL ROCA que esta tanto en RÍO NEGRO como en CÓRDOBA.
Por eso también tenemos que tener en cuenta las provincias a la hora de vincular los df
"""

# Tercero. Dado que en el df1 las provincias estan en mayuscula, ponemos en mayusculas las de df3

df3['provincia'] = df3['provincia'].str.upper()

"""
Nuevamente hariamos un merge en este momento pero existe otro problema. Los nombres de departamentos en df1 a veces aparecen
con nombres de sus localidades, las cuales estan en el df3. Entonces, podemos hacer un for que busque si coinciden el nombre
que aparece en df1 con los nombres de localidades de df3, y si matchean cambiar ese nombre de localidad que esta en df1 por
el verdadero nombre de departamento que le corresponde
"""

# Cuarto. Ponemos en mayusculas los nombres y hacemos una copia del df1.

df3['nombre'] = df3['nombre'].str.upper()
df1_corregido = df1.copy() # Creamos un df1_corregido que será el que tenga los departamentos bien nombrados sin modificar el df1

# Quinto. Existe un único valor NaN en la fila 60. Para corroborar podemos usar df3.loc[df3.departamento.isna(),'departamento']
# Lo corregimos a mano

df3.loc[60,'departamento'] = sin_definir

# Sexto. Luego aplicamos quitar_acentos en los nombres de departamento del df1, df3 y la columna 'nombre' del df3
# Esto es para que sean más faciles de relacionar entre sí por más que perdamos calidad de escritura

df3['departamento'] = df3['departamento'].apply(quitar_acentos)
df1_corregido['departamento'] = df1_corregido['departamento'].apply(quitar_acentos)
df3['nombre'] = df3['nombre'].apply(quitar_acentos)

# Séptimo. Podemos hacer un for que recorra cada departamento del df1 y lo compare con los nombres de localidades que aparecen en
# df3 y si llega a encontrar un match, eso significa que ese departamento del df1 esta mal y es una localidad en realidad.
# A continuación buscamos en la columna 'departamento' del df3 en la misma fila que hizo match y reemplazamos ese valor en el df1

for departamento in df1_corregido['departamento']:
    if departamento in df3['nombre'].values: # Verificar si hay coincidencia en la df3['nombre']
        nombre_departamento = df3.loc[df3['nombre'] == departamento, 'departamento'].values[0] # Obtener el valor correspondiente de df3['departamento']
        df1_corregido.loc[df1_corregido['departamento'] == departamento, 'departamento'] = nombre_departamento  # Reemplazar el valor en df1['departamento']

del departamento # para que no queden variables innecesarias 

# Octavo. Hacemos el merge

df1_corregido= df1_corregido.merge(df3[['provincia_id','departamento','departamento_id']].drop_duplicates() , on=['provincia_id','departamento'], how='left')

# Noveno. Por último ponemos los id al lado de los departamentos

df1_corregido.insert(4,'departamento_id',df1_corregido.pop('departamento_id'))

"""
En total df1_corregido tiene 435 NaNs. Si bien todavia quedan esa cantidad de NaNs, originalmente el df1 no tenia IDs de 
departamentos, y ahora tiene 960 filas que si tienen id de departamento.
Esto es verificable con el siguiente comando.
"""
df1_corregido.loc[df1_corregido.departamento_id.isna(),'departamento_id']

""" 
Ahora otra cosa aparte es que podemos inferir la localidad (que la mayoria aparecen como NaN corregidos a INDENIFINIDO) a partir
del df3. Asi que hacemos eso a continuación.
"""

# Una cosa a destacar es que la idea seria vincular departamentos del df1 con departamentos del df3, y en la fila que matchee
# quedarnos con la localidad y reemplazarla en el df1. Facil. Pero el problema es que cada departamento tiene varias localidades
# Asi que ¿cual de todas nos quedamos?. Nosotros decidimos siempre utilizar aquellas localidades que tienen como función ser
# CABECERA_DEPARTAMENTO. Ya que no existe otra manera de averiguar cual de todas las localidades posibles poner en INDEFINIDO.

for departamento in df1_corregido['departamento']:
    if departamento in df3['departamento'].values & df3.loc['departamento' == departamento,'funcion']=='CABECERA_DEPARTAMENTO' : # Verificar si hay coincidencia de departamentos y si la localidad es CABECERA_DEPARTAMENTO
        nombre_localidad= df3.loc[df3['departamento'] == departamento, 'nombre'].values[0] # Obtener el valor correspondiente de df3['departamento']
        df1_corregido.loc[df1_corregido['departamento'] == departamento, 'localidad'] = nombre_localidad  # Reemplazar el valor en df1['departamento']

#%%
#-----------------------------------JUAN PABLO-------------------------------

df1_corregido_fails=df1_corregido[df1_corregido.departamento_id.isna()]

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

