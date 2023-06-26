import pandas as pd
from inline_sql import sql, sql_val
#%%
# Carga de dataframes
vuelo      = pd.read_csv("vuelo.csv")    
aeropuerto = pd.read_csv("aeropuerto.csv")    
pasajero   = pd.read_csv("pasajero.csv")    
reserva    = pd.read_csv("reserva.csv")

# Ejercicio JOIN tuplas espúreas
empleadoRol= pd.read_csv("empleadoRol.csv")    
rolProyecto= pd.read_csv("rolProyecto.csv") 
# Funciones de Agregacion
examen     = pd.read_csv("examen.csv")
examen03    = pd.read_csv("examen03.csv")
# OPERACIONES ENTRE DATABASES

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

imprimirEjercicio(consigna, [operador,departamento,provincia], consultaSQL)
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

imprimirEjercicio(consigna, [operador,departamento,provincia], consultaSQL)

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

imprimirEjercicio(consigna, [operador,departamento,provincia], consultaSQL)