import numpy as np
import requests
import pandas as pd
from sqlalchemy import create_engine
from polyfuzz import PolyFuzz
from polyfuzz.models import RapidFuzz

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)  # Ajustar el ancho de la consola

#Extraer los datos a partir de la API con máximo 50.000 registros
url='https://www.datos.gov.co/resource/6cat-2gcs.json?$limit=50000'

response=requests.get(url)
response_json=response.json()

#Crear un Dataframe a partir del formato json
df=pd.DataFrame(response_json)


                                        #Análisis exploratorio

'''Se identifican nombres de columnas erróneos'''
df.columns=df.columns.str.strip()
df=df.rename(columns={'nit':'NIT','raz_n_social':'Razon_social','regi_n':'Region','supervisor':'Supervisor',
                   'departamento_domicilio':'departamento_domicilio','ciudad_domicilio':'Ciudad_domicilio',
                   'ciiu':'CIIU','macrosector':'Macrosector','ingresos_operacionales':'Ingresos_operacionales',
                      'ganancia_p_rdida':'Ganancia_perdida','total_activos':'Total_activos',
                      'total_pasivos':'Total_pasivos', 'total_patrimonio':'Total_patrimonio',
                      'a_o_de_corte':'Anio_de_corte'})

#information=df.info()
'''No hay presencia de registros faltantes. Sin embargo, todos los tipos de datos son tipo string'''

#Limpiar las columnas con valores en moneda
'''Debe eliminarse el símbolo $ con el fin de transformar el tipo de dato y efectuar cálculos posteriores '''

col_num=['Ingresos_operacionales',
         'Ganancia_perdida',
         'Total_activos',
         'Total_pasivos',
         'Total_patrimonio'
         ]

for col in col_num:
    df[col]=df[col].str.replace('$','',regex=False)
    df[col]=df[col].astype('float')

df['Anio_de_corte']=df['Anio_de_corte'].astype('int')
df['NIT']=df['NIT'].astype('int')


#Analizar las estadísticas descriptivas
description=df.describe()
'''Con base en las cifras anteriores, existen un outlier llamativo que distorsiona el promedio de ingresos del total de
    empresas.'''


#Estandarizar nombre de departamentos
df['departamento_domicilio']=df['departamento_domicilio'].str.replace('Guajira','La Guajira',regex=False)

#Crear margen neto
df['Margen_neto']=round((df['Ganancia_perdida']/df['Ingresos_operacionales'].replace(0,np.nan))*100,2)

'''Algunos valores en el denominador de las siguientes columnas nuevas contienen cero, lo que da infinito.
Por lo tanto se convierten a NaN para evitar sesgos en cálculos estadísticos'''

#Crear índice de endeudamiento
df['indice_endeudamiento']=round((df['Total_pasivos']/df['Total_activos'].replace(0,np.nan))*100,2)

#Calcular los ratios de rentabilidad
#ROA
df['ROA']=round((df['Ganancia_perdida']/df['Total_activos'].replace(0,np.nan))*100,2)

#ROE
df['ROE']=round((df['Ganancia_perdida']/df['Total_patrimonio'].replace(0,np.nan))*100,2)

#Multiplicador del capital
df['multiplicador_capital']=round(df['Total_activos']/df['Total_patrimonio'].replace(0,np.nan),2)

#Eliminar columnas innecesarias
df=df.drop(columns=['NIT','Region'])

#Convertir los datos categóricos en título
col_cat=['Supervisor',
         'departamento_domicilio',
         'Ciudad_domicilio','Macrosector']

for col in col_cat:
    df[col]=df[col].str.lower()
    df[col]=df[col].str.title()


#Crear tablas dimensiones
empresas=df[['Razon_social']]
empresas=empresas.drop_duplicates().reset_index(drop=True)

#Crear df temporal para comenzar limpieza de nombres de empresas
empresas['nombre_limpio']=empresas['Razon_social'].str.lower()
empresas['nombre_limpio']=empresas['nombre_limpio'].str.replace(r'[\.,/();%$@¡]','',regex=True)
empresas['nombre_limpio']=empresas['nombre_limpio'].str.replace(r'&','y',regex=True)
empresas['nombre_limpio']=empresas['nombre_limpio'].str.replace(r'-',' ',regex=True)
empresas['nombre_limpio']=empresas['nombre_limpio'].str.replace(r'\b(sas|sa|ltda|ltd|limitada|s a|'
                                                                r'colombia|bic|ingenieria|ingeniería|'
                                                                r'esp|ese|reorganizacion|group|'
                                                                r'colombi|reorganización|liquidacion|'
                                                                r'liquidación|llc|construccion|'
                                                                r'construcción|sucursal|sucrusal|'
                                                                r'sca|en)\b','',regex=True).str.strip()

#Utilizar Fuzzy String Matching para comparar cadenas de texto similares con el fin de estandarizar los nombres
datos_originales=empresas['nombre_limpio'].unique().tolist()
rapid_fuzz=RapidFuzz(n_jobs=-1)
model=PolyFuzz(rapid_fuzz)
model.match(datos_originales,datos_originales)
model.group(link_min_similarity=0.94)
clusters=model.get_clusters()

map_lista=[]
for id, nombres in clusters.items():
    #Anexar el segundo nombre de la agrupación como nombre oficial para cada organización
    nombre_oficial=nombres[0]
    for n in nombres:
        map_lista.append({
            'nombre_limpio':n,
            'nombre_normalizado':nombre_oficial
        })
df_map=pd.DataFrame(map_lista)

empresas_limpia=pd.merge(empresas,df_map,on='nombre_limpio',how='left')
empresas_limpia['nombre_normalizado']=empresas_limpia['nombre_normalizado'].fillna(empresas_limpia['nombre_limpio'])
empresas_limpia=empresas_limpia.drop(columns='nombre_limpio').reset_index(drop=True)
empresas_limpia['id_empresa']=empresas_limpia.index+1
empresas_limpia['id_empresa']=empresas_limpia['id_empresa'].astype('int')


supervisor=df[['Supervisor']]
supervisor=supervisor.drop_duplicates().reset_index(drop=True)
supervisor['id_supervisor']=supervisor.index+1

geografia=df[['Ciudad_domicilio','departamento_domicilio']]
geografia=geografia.drop_duplicates().reset_index(drop=True)
geografia['id_ciudad']=geografia.index+1

macrosector=df[['Macrosector']]
macrosector=macrosector.drop_duplicates().reset_index(drop=True)
macrosector['id_macrosector']=macrosector.index+1

anio_corte=df[['Anio_de_corte']]
anio_corte=anio_corte.drop_duplicates().reset_index(drop=True)
anio_corte['id_anio']=anio_corte.index+1

#Crear tabla hechos
fact_table=pd.merge(df,empresas_limpia,on='Razon_social',how='left')
fact_table=pd.merge(fact_table,supervisor,on='Supervisor',how='left')
fact_table=pd.merge(fact_table,geografia,on='Ciudad_domicilio',how='left')
fact_table=pd.merge(fact_table,macrosector,on='Macrosector',how='left')
fact_table=pd.merge(fact_table,anio_corte,on='Anio_de_corte',how='left')

#Eliminar duplicados en la tabla empresas
empresas_limpia=empresas_limpia.drop_duplicates(subset='nombre_normalizado')

#Estandarizar nombre de empresas
empresas_limpia=empresas_limpia.rename(columns={'nombre_normalizado':'empresa'})
empresas_limpia['empresa']=empresas_limpia['empresa'].str.title()
empresas_limpia=empresas_limpia.drop(columns='Razon_social')

#Eliminar duplicados de empresas en fact_table para que solo quede una empresa inscrita en una ciudad y por año
fact_table=fact_table.drop_duplicates(subset=['Razon_social',
                                              'Ciudad_domicilio',
                                              'Anio_de_corte'],keep='first')

#Eliminar columnas innecesarias en la tabla hechos
fact_table=fact_table.drop(columns=['Razon_social',
                                    'Supervisor','departamento_domicilio_x',
                                    'Ciudad_domicilio','CIIU','Macrosector',
                                    'nombre_normalizado','departamento_domicilio_y'
                                                                            ])

#Crear la conexión con la base de datos
user='root'
password=''
host=''
puerto='3306'
database='empresas'

#Crear motor de conexión
engine=create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

#Enviar las tablas a MySQL
empresas_limpia.to_sql(name='empresas',con=engine,if_exists='replace',index=False)
supervisor.to_sql(name='supervisor',con=engine,if_exists='replace',index=False)
geografia.to_sql(name='geografia',con=engine,if_exists='replace',index=False)
macrosector.to_sql(name='macrosector',con=engine,if_exists='replace',index=False)
anio_corte.to_sql(name='anio_corte',con=engine,if_exists='replace',index=False)
fact_table.to_sql(name='tabla_hechos',con=engine,if_exists='replace',index=False)
