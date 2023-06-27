import pandas as pd
from inline_sql import sql
import matplotlib.pyplot as plt

#%%
# Carga de dataframes
operador     = './TablasLimpias/operador.csv'  
salario      = './TablasLimpias/salario.csv'
departamento = './TablasLimpias/departamento.csv'
provincia    = './TablasLimpias/provincia.csv'
clase        = './TablasLimpias/clase.csv'

operador = pd.read_csv(operador)
salario = pd.read_csv(salario)
departamento = pd.read_csv(departamento)
provincia = pd.read_csv(provincia)
clase = pd.read_csv(clase)

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

# =============================================================================
# Ejercicio j:
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
"""
Ejercicio 1: Cantidad de Operadores por provincia.
"""
operadores_por_provincia        = pd.merge(operador, departamento, on='id_departamento', how='inner')
operadores_con_nombre_provincia = pd.merge(operadores_por_provincia, provincia, on='id_provincia', how='inner')
ocurrencias_por_provincia = operadores_con_nombre_provincia.nombre_provincia.value_counts()
# Calcular el total de ocurrencias
total_ocurrencias = ocurrencias_por_provincia.sum()
# Calcular los porcentajes de ocurrencia
porcentajes = (ocurrencias_por_provincia / total_ocurrencias) * 100
# Crear la figura y el eje del gráfico
fig, ax = plt.subplots()
provincias = [str(d) for d in ocurrencias_por_provincia.index]
values = ocurrencias_por_provincia.values
plt.bar(provincias,values)
#Agregar etiquetas de texto en cada barra
for i in range(len(ocurrencias_por_provincia)):
    ax.text(i , values[i],
            f"{porcentajes.values[i]:.2f}%",
            ha='center',
            va='top',
            rotation=60,
            c="azure")
# Configurar etiquetas y título del gráfico
ax.set_xlabel('Provincia')
ax.set_ylabel('Ocurrencias')
ax.set_title("Cantidad de operadores por provincia")
plt.savefig('./Graficos/operadores_por_provincia.png')
plt.show()
plt.close()
#%%