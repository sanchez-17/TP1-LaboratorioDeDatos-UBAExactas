import pandas as pd
from inline_sql import sql, sql_val
#%%
# Carga de dataframes
operador     = pd.read_csv("vuelo.csv")    
salario      = pd.read_csv("aeropuerto.csv")    
departamento = pd.read_csv("pasajero.csv")    
provincia    = pd.read_csv("reserva.csv")

#%%
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
# EJERCICIOS
# =============================================================================
#%% EJERCICIO 1

consigna = """Ejercicio 1:\n
                ¿Existen provincias que no presentan Operadores Orgánicos Certificados?\n
                ¿En caso de que sí, cuántas y cuáles son?
"""    

consultaSQL = """
                SELECT DISTINCT
                   count(*)                AS cant_operadores,
                   pr.nombre_provincia     AS provincia
                   FROM operador           AS op
                   INNER JOIN departamento AS depto
                   ON op.id_departamento = depto.id_departamento
                   RIGHT OUTER JOIN provincia AS pr
                   ON depto.id_provincia = pr.id_provincia 
                   GROUP BY provincia
                   HAVING cant_operadores = 0;
              """

imprimirEjercicio(consigna, [operador,departamento,provincia], consultaSQL)
#%%
# Ejericicio 2

consigna = """Ejercicio 2:\n
                ¿Existen departamentos que no presentan Operadores Orgánicos
                Certificados?\n
                 ¿En caso de que sí, cuántos y cuáles son?
"""    
consultaSQL = """
                SELECT DISTINCT
                   count(*)      AS cant_operadores, 
                   depto.nombre  AS departamento
                   FROM operador AS op
                   RIGHT OUTER JOIN departamento AS depto
                   ON op.id_departamento = depto.id_departamento
                   GROUP BY departamento
                   HAVING cant_operadores = 0;
              """

imprimirEjercicio(consigna, [operador,departamento], consultaSQL)
#%%
# Ejericicio 3

consigna = """Ejercicio 3:\n
                ¿Cuál es la actividad que más operadores tiene?
"""    
consultaSQL = """
                SELECT              
                   MAX(cant_operadores)   AS maxima_cant_operadores,
                   ANY_VALUE(descripcion) AS descripcion_clase,
                   ANY_VALUE(id_clase)    AS id_clase
                   FROM (
                     SELECT
                     count(op.id_operador) AS cant_operadores,
                     clase.descripcion     AS descripcion,
                     clase.id_clase        AS id_clase
                     FROM operador         AS op
                     RIGHT OUTER JOIN clase
                     ON op.id_clase = clase.id_clase
                     GROUP BY clase.id_clase, clase.descripcion
                   )
              """

imprimirEjercicio(consigna, [operador,clase], consultaSQL)

#%%
# Ejericicio 4

consigna = """Ejercicio 4:\n
                ¿Cuál fue el salario promedio de esa actividad en 2022?\n
                (si hay varios registros de salario, mostrar el más actual de ese año)
"""    
consultaSQL = """
                SELECT
                   ROUND(AVG(salario), 2) as promedio_anual,
                   FROM (
                     SELECT 
                     salario_promedio AS salario,
                     FROM salario
                     WHERE id_clase = 10 AND anio = 2022
                   )
              """

imprimirEjercicio(consigna, [salario], consultaSQL)
#%%
# Ejericicio 5

consigna = """Ejercicio 5:\n
                ¿Cuál es el promedio anual de los salarios en Argentina y cual es su desvío?\n
                ¿Y a nivel provincial? ¿Se les ocurre una forma de que sean comparables a lo largo de los años?\n
                ¿Necesitarían utilizar alguna fuente de datos externa secundaria? ¿Cuál?
"""    
consultaSQL = """
                SELECT
                   ROUND(AVG(salario_promedio), 2) as promedio,
                   prov.nombre_provincia
                   FROM salario
                   INNER JOIN operador AS op
                   ON salario.id_clase = op.id_clase
                   INNER JOIN departamento AS depto
                   ON op.id_departamento = depto.id_departamento
                   RIGHT OUTER JOIN provincia AS prov
                   ON depto.id_provincia = prov.id_provincia
                   GROUP BY anio, prov.nombre_provincia
                   ORDER BY anio ASC, prov.nombre_provincia ASC
              """

imprimirEjercicio(consigna, [salario,operador,departamento,provincia], consultaSQL)