"""
TP1LaboDatos.ipynb

Grupo: AltaData

Integrantes:Gaston Sanchez, Mariano Papaleo, Juan Pablo Hugo Aquilante

#Preguntas de ejercicios i), j)

"""
import pandas as pd
from inline_sql import sql
import matplotlib.pyplot as plt
from funciones import *
import seaborn as sns

#%%
# Carga de dataframes
operador     = './TablasLimpias/operador.csv'  
salario      = './TablasLimpias/salario.csv'
departamento = './TablasLimpias/departamento.csv'
provincia    = './TablasLimpias/provincia.csv'
clase        = './TablasLimpias/clase.csv'
producto     = './TablasLimpias/producto.csv'

operador = pd.read_csv(operador)
salario = pd.read_csv(salario)
departamento = pd.read_csv(departamento)
provincia = pd.read_csv(provincia)
clase = pd.read_csv(clase)
producto = pd.read_csv(producto)

#%%
# =============================================================================
# EJERCICIOS
# =============================================================================
#%% EJERCICIO 1

consigna = """Ejercicio 1:\n
                ¿Existen provincias que no presentan Operadores Orgánicos Certificados?\n
                ¿En caso de que sí, cuántas y cuáles son?
"""    
#Vemos la cant por provincia y luego filtramos los que tienen valor 0
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
                   ORDER BY cant_operadores DESC
              """
              
cant_operadores_por_provincia = sql ^ consultaSQL
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
              
provincias_sin_operadores = sql ^ consultaSQL
print("Cantidad de provincias sin operadores organicos certificados: ",len(provincias_sin_operadores))
imprimirEjercicio(consigna, [operador,departamento,provincia], consultaSQL)

#%%
# Ejericicio 2

consigna = """Ejercicio 2:\n
                ¿Existen departamentos que no presentan Operadores Orgánicos
                Certificados?\n
                ¿En caso de que sí, cuántos y cuáles son?
"""    
consultaSQL = """
                SELECT COUNT(o.id_operador) AS cant_operadores, d.nombre_departamento
                FROM departamento d
                LEFT JOIN operador o ON d.id_departamento = o.id_departamento
                GROUP BY d.id_departamento, d.nombre_departamento
                HAVING cant_operadores = 0
              """
departamentos_sin_operadores = sql ^ consultaSQL
departamentos_sin_operadores.to_csv('./Graficos/departamentos_sin_operadores.csv', index=False)
lista_depas = departamentos_sin_operadores.loc[:,departamentos_sin_operadores.columns[1]].values
imprimirEjercicio(consigna, [operador,departamento], consultaSQL)
print("Cantidad de departamentos sin operadores organicos certificados: ",len(departamentos_sin_operadores))
#%%
# Ejericicio 3
consigna = """Ejercicio 3:\n
                ¿Cuál es la actividad que más operadores tiene?
"""    
#Vemos por actividad y luego agarramos el maximo
consultaSQL = """
                SELECT
                count(op.id_operador) AS cant_operadores,
                clase.descripcion     AS descripcion,
                clase.id_clase        AS id_clase
                FROM operador         AS op
                RIGHT OUTER JOIN clase
                ON op.id_clase = clase.id_clase
                GROUP BY clase.id_clase, clase.descripcion
                ORDER BY cant_operadores DESC
                   
              """
cant_operadores_por_actividad = sql ^ consultaSQL#maxima actividad id_clave = 1
imprimirEjercicio(consigna, [operador,clase], consultaSQL)

#%%
# Ejericicio 4

consigna = """Ejercicio 4:\n
                ¿Cuál fue el salario promedio de esa actividad en 2022?\n
                (si hay varios registros de salario, mostrar el más actual de ese año)
"""    
consultaSQL = """
                SELECT
                   ROUND(AVG(salario_promedio), 2) as promedio_anual,
                   FROM (
                     SELECT 
                     salario_promedio,
                     FROM salario s
                     WHERE s.id_clase = 1 AND s.anio = 2022
                   )
              """
salario_prom_act1_2022 = sql ^ consultaSQL #94261.49
imprimirEjercicio(consigna, [salario], consultaSQL)
print("Salario promedio 2022 de la actividad con mas operadores: $",*(salario_prom_act1_2022.values)[0])
#%%
#Salario promedio para todas las actividades en 2022 por provincia
consultaSQL="""
                    SELECT
                        s.id_provincia,
                        p.nombre_provincia,
                        s.anio,
                        AVG(s.salario_promedio) AS promedio_anual,
                        STDDEV(salario_promedio) as desvio_estandar
                    FROM
                        salario s
                    JOIN
                        provincia p ON s.id_provincia = p.id_provincia
                    INNER JOIN operador as o
                        ON o.id_clase = s.id_clase
                    WHERE
                        s.anio = 2022 AND s.mes = 12
                    GROUP BY
                        s.id_provincia,
                        p.nombre_provincia,
                        s.anio
"""
salario_prom_provincial_2022 = sql ^ consultaSQL 
# Ejericicio 5

consigna = """Ejercicio 5:\n
                ¿Cuál es el promedio anual de los salarios en Argentina y cual es su desvío?\n
                ¿Y a nivel provincial? ¿Se les ocurre una forma de que sean comparables a lo largo de los años?\n
                ¿Necesitarían utilizar alguna fuente de datos externa secundaria? ¿Cuál?
"""
#Promedio salarial con desvio por año en Argentina 
consultaSQL = """SELECT
                    ROUND(AVG(salario_promedio), 2) as promedio_anual,
                    STDDEV(salario_promedio) as desvio_estandar,
                    anio
                    FROM salario as s
                    INNER JOIN operador as o
                    ON o.id_clase = s.id_clase
                    GROUP BY anio
                    ORDER BY promedio_anual DESC
              """
prom_anual_salarios_nacional = sql ^ consultaSQL
#%%
#Promedio anual de salarios por provincia
consultaSQL = """
                SELECT
                   ROUND(AVG(salario_promedio), 2) as promedio_anual,
                   ROUND(STDDEV(salario_promedio), 2) as desvio,
                   prov.nombre_provincia AS nombre_provincia, salario.anio AS anio
                   FROM salario
                   INNER JOIN operador AS op
                   ON salario.id_clase = op.id_clase
                   INNER JOIN departamento AS depto
                   ON op.id_departamento = depto.id_departamento
                   RIGHT OUTER JOIN provincia AS prov
                   ON depto.id_provincia = prov.id_provincia
                   GROUP BY anio, prov.nombre_provincia
                   ORDER BY promedio_anual DESC
              """
prom_anual_salarios_provincial = sql ^ consultaSQL
#%%
#Vemos las 3 principales
#Buenos Aires
consultaSQL = """
                SELECT
                   ROUND(AVG(salario_promedio), 2) as promedio_anual,
                   ROUND(STDDEV(salario_promedio), 2) as desvio,
                   prov.nombre_provincia AS nombre_provincia, salario.anio AS anio
                   FROM salario
                   INNER JOIN operador AS op
                   ON salario.id_clase = op.id_clase
                   INNER JOIN departamento AS depto
                   ON op.id_departamento = depto.id_departamento
                   RIGHT OUTER JOIN provincia AS prov
                   ON depto.id_provincia = prov.id_provincia
                   WHERE prov.nombre_provincia = 'BUENOS AIRES'
                   GROUP BY anio, prov.nombre_provincia
                   ORDER BY promedio_anual DESC
              """

buenos_aires = sql ^ consultaSQL
buenos_aires.to_csv('./Graficos/buenosAires.csv', index=False)
#%%
#Misiones
consultaSQL = """
                SELECT
                   ROUND(AVG(salario_promedio), 2) as promedio_anual,
                   ROUND(STDDEV(salario_promedio), 2) as desvio,
                   prov.nombre_provincia AS nombre_provincia, salario.anio AS anio
                   FROM salario
                   INNER JOIN operador AS op
                   ON salario.id_clase = op.id_clase
                   INNER JOIN departamento AS depto
                   ON op.id_departamento = depto.id_departamento
                   RIGHT OUTER JOIN provincia AS prov
                   ON depto.id_provincia = prov.id_provincia
                   WHERE prov.nombre_provincia = 'MISIONES'
                   GROUP BY anio, prov.nombre_provincia
                   ORDER BY promedio_anual DESC
              """

misiones = sql ^ consultaSQL
misiones.to_csv('./Graficos/misiones.csv', index=False)

#%%
#Mendoza
consultaSQL = """
                SELECT
                   ROUND(AVG(salario_promedio), 2) as promedio_anual,
                   ROUND(STDDEV(salario_promedio), 2) as desvio,
                   prov.nombre_provincia AS nombre_provincia, salario.anio AS anio
                   FROM salario
                   INNER JOIN operador AS op
                   ON salario.id_clase = op.id_clase
                   INNER JOIN departamento AS depto
                   ON op.id_departamento = depto.id_departamento
                   RIGHT OUTER JOIN provincia AS prov
                   ON depto.id_provincia = prov.id_provincia
                   WHERE prov.nombre_provincia = 'MENDOZA'
                   GROUP BY anio, prov.nombre_provincia
                   ORDER BY promedio_anual DESC
              """

mendoza = sql ^ consultaSQL
mendoza.to_csv('./Graficos/mendoza.csv', index=False)
#%%
filtro = prom_anual_salarios_provincial.nombre_provincia.isin(["BUENOS AIRES", "MENDOZA", "MISIONES"])
resultados_filtrados = prom_anual_salarios_provincial[filtro]
#%%
#Generamos un promedio por provincia y por anio. Luego tomamos desvio estandar agrupando por provincia

consultaSQL = """SELECT
                    ROUND(STDDEV(promedio_anual), 2) as desvio,
                    nombre_provincia
                    FROM(
                        SELECT
                           ROUND(AVG(salario_promedio), 2) as promedio_anual,
                           prov.nombre_provincia AS nombre_provincia, salario.anio AS anio
                           FROM salario
                           INNER JOIN operador AS op
                           ON salario.id_clase = op.id_clase
                           INNER JOIN departamento AS depto
                           ON op.id_departamento = depto.id_departamento
                           RIGHT OUTER JOIN provincia AS prov
                           ON depto.id_provincia = prov.id_provincia
                           GROUP BY anio, prov.nombre_provincia
                           ORDER BY promedio_anual DESC
                       )
                    GROUP BY nombre_provincia
              """
desvio_prom_anual_por_provincia = sql ^ consultaSQL
#%%
imprimirEjercicio(consigna, [salario,operador,departamento,provincia], consultaSQL)

#%%
# =============================================================================
# Ejercicio j.1: Cantidad de Operadores por provincia.
# =============================================================================
operadores_por_provincia        = pd.merge(operador, departamento, on='id_departamento', how='inner')
operadores_con_nombre_provincia = pd.merge(operadores_por_provincia, provincia, on='id_provincia', how='inner')
ocurrencias_por_provincia = operadores_con_nombre_provincia.nombre_provincia.value_counts()
# Calcular el total de ocurrencias
total_ocurrencias = ocurrencias_por_provincia.sum()
# Calcular los porcentajes de ocurrencia
porcentajes = (ocurrencias_por_provincia / total_ocurrencias) * 100
# Crear la figura y el eje del gráfico
fig, ax = plt.subplots(figsize=(18,5))
provincias = [str(d) for d in ocurrencias_por_provincia.index]
values = ocurrencias_por_provincia.values
provincias[10] ="CABA"
plt.bar(provincias,values)
plt.xticks(rotation=45)
#Agregar etiquetas de texto en cada barra
for i,v in enumerate(values):
    ax.text(i , v,
            f"{porcentajes.values[i]:.2f}%",
            ha='center',
            va='bottom',
            #rotation=60,
            c="black")
# Configurar etiquetas y título del gráfico
ax.set_xlabel('Provincia')
ax.set_ylabel('Ocurrencias')
ax.set_title("Cantidad de operadores por provincia")
plt.savefig('./Graficos/operadores_por_provincia.png')
plt.show()
plt.close()
#%%
# =============================================================================
# Ejercicio j.2: Boxplot, por cada provincia, donde se pueda observar la cantidad de
# productos por operador.
# =============================================================================
consultaSQL = """
                   SELECT 
                    id_operador AS id, 
                    count(*) AS cantidad_productos
                   FROM producto
                   GROUP BY id_operador;
              """
operador_cant_prod = sql ^ consultaSQL
#%%
consultaSQL = """
                   SELECT DISTINCT 
                        oper.id_operador, 
                        oper.nombre_provincia, 
                        prod.cantidad_productos
                   FROM operadores_con_nombre_provincia oper
                   INNER JOIN operador_cant_prod prod
                   ON oper.id_operador=prod.id
                   ORDER BY oper.id_operador
              """
operador_prov_cant_prod = sql ^ consultaSQL
#%%
operador_prov_filtrado = operador_prov_cant_prod.query('cantidad_productos <= 12')

sns.boxplot(data=operador_prov_filtrado, y="nombre_provincia", x="cantidad_productos").set(
    ylabel="Provincia", xlabel="Cantidad de productos"
)
plt.savefig('./Graficos/boxPlotEj2.png')
plt.show()
plt.close()
#%% Con plt
# df = operador_prov_filtrado
# plt.boxplot([df[df['nombre_provincia'] == value]['cantidad_productos'] for value in sorted(df['nombre_provincia'].unique())], vert=False)
# plt.yticks(range(1, 25), sorted(df['nombre_provincia'].unique()))
# plt.title("Box Plot")
# plt.xlabel("cantidad de productos")
# plt.ylabel("provincia")
# plt.savefig('./Graficos/boxPlotEj2.png')
# plt.show()
#%%
# =============================================================================
# Ejercicio 3: Relación entre la cantidad de operadores y el salario promedio en cada provincia de ARG 
#            en el año 2022 y con un promedio de salario del último mes
# =============================================================================
consultaSQL="""
                    SELECT
                        s.id_provincia,
                        p.nombre_provincia,
                        s.anio,
                        AVG(s.salario_promedio) AS promedio_anual,
                        STDDEV(salario_promedio) as desvio_estandar
                    FROM
                        salario s
                    JOIN
                        provincia p ON s.id_provincia = p.id_provincia
                    INNER JOIN operador as o
                        ON o.id_clase = s.id_clase
                    WHERE
                        s.anio = 2022 AND s.mes = 12
                    GROUP BY
                        s.id_provincia,
                        p.nombre_provincia,
                        s.anio
"""
salario_prom_provincial= sql ^ consultaSQL

salario_prom_provincial.at[0,'nombre_provincia'] = 'CABA'
cant_operadores = pd.DataFrame({'provincia': provincias, 'cant_operadores': values})
cant_operadores.rename(columns={'provincia':'nombre_provincia'},inplace = True)

salario_prom_provincial = salario_prom_provincial.merge(cant_operadores[['nombre_provincia','cant_operadores']], 
                                                        on = 'nombre_provincia',how ='left')

fig, ax = plt.subplots(figsize=(10, 10))
sns.scatterplot(data = salario_prom_provincial , x = 'cant_operadores', y = 'promedio_anual',hue='nombre_provincia',palette ='rainbow')
ax.set_xlabel('cantidad operadores', fontsize=16)
ax.set_ylabel('salario promedio último de 2022', fontsize=16)

plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), title='Leyenda', handlelength=2.0)
plt.grid(True)

plt.savefig('./Graficos/ScatterEj3.png')
plt.show()
plt.close() 
#%%
# =============================================================================
# Ejercicio j.4: Salario promedio por provincia en ARG.
# =============================================================================

salario.loc[len(salario)-1,['anio','mes']] # Ultimo ingreso medio = Enero 2023

provincia.at[0,'nombre_provincia'] = 'CABA'
salario_provincia = pd.merge(salario, provincia, on='id_provincia', how='inner')
#desestimamos aquellos valores negativos
salario_p = salario_provincia[salario_provincia['salario_promedio'] != -99]
salario_2022 = salario_p[salario_p['anio'] == 2023]
salario_enero = salario_2022[salario_2022['mes'] == 1]

quantil = salario_enero.loc[:, "salario_promedio"].quantile(0.97)
salario_enero_prom = salario_enero[salario_enero['salario_promedio'] < quantil]

plt.ylim(0, quantil)

#Generar el gráfico de violín
sns.violinplot(data = salario_enero_prom , x = 'salario_promedio' , y = 'nombre_provincia' )
ax.set_xlabel('Salario promedio', fontsize=18)
ax.set_ylabel('Provincia', fontsize=16)
plt.savefig('./Graficos/ViolinPlotEj4.png')
plt.show()
plt.close()