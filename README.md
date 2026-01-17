<h3 align="center"> 🏭 Situación financiera de las 15.000 empresas más grandes de Colombia </h3>

<p align="center">
  <img width="554" height="170" alt="Sin título" src="https://github.com/user-attachments/assets/f261927b-56d1-4e58-ba6e-562704f415e0" />
</p>

## 🎯 Descripción del Proyecto
Este proyecto ha sido creado con el propósito de obtener valor sobre los datos financieros de las 15.000 empresas más grandes de Colombia. Esta información es ofrecida por la Superintendencia de Sociedades, la cual reporta de forma anual los balances financieros de las 15.000 con mayor relevancia económica para un período específico comprendido entre los años 2021 a 2024. El objetivo es identificar factores relevantes y tendencias relacionadas con su distribución geográfica, macrosector y año de estudio, agregando a su vez al análisis indicadores económicos claves que permitan generar una "radiografía" de su estados contables.

## 🛠️ Stack Tecnológico
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![SQL](https://img.shields.io/badge/sql-%2307405e.svg?style=for-the-badge&logo=mysql&logoColor=white)
![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=microsoftpowerbi&logoColor=black)
![API](https://img.shields.io/badge/API-REST-orange?style=for-the-badge&logo=api&logoColor=white)

## Índice
1. [Diccionario de campos](#diccionario)
2. [Directorio](#directorio)
3. [Propósito del proyecto](#proposito)
4. [Arquitectura de Datos](#arquitectura)
5. [Instalación y Uso](#instalación)
6. [Análisis y Hallazgos](#análisis)


<a name="diccionario"></a>
## 📂 Diccionario de campos


| Campo (Interfaz) | Nombre Técnico (API) | Tipo | Descripción |
| :--- | :--- | :--- | :--- |
| **NIT** | `nit` | Número | Número de Identificación Tributaria de la Sociedad. |
| **GANANCIA (PÉRDIDA)** | `ganancia_p_rdida` | Texto* | Ganancias registradas por la sociedad. |
| **TOTAL ACTIVOS** | `total_activos` | Texto* | Total activos registrados por la sociedad. |
| **TOTAL PASIVOS** | `total_pasivos` | Texto* | Total Pasivos registrados por la sociedad. |
| **TOTAL PATRIMONIO** | `total_patrimonio` | Texto* | Total Patrimonio registrado por la sociedad. |
| **Año de Corte** | `a_o_de_corte` | Número | Fecha en que finaliza el periodo contable. |
| **RAZÓN SOCIAL** | `raz_n_social` | Texto | Nombre de la sociedad. |
| **SUPERVISOR** | `supervisor` | Texto | Empresa que ejerce supervisión. |
| **REGIÓN** | `regi_n` | Texto | Región Geográfica de la sociedad. |
| **DEPARTAMENTO** | `departamento_domicilio` | Texto | Departamento de domicilio. |
| **CIUDAD** | `ciudad_domicilio` | Texto | Ciudad de domicilio. |
| **CIIU** | `ciiu` | Número | Clasificación Industrial Internacional Uniforme. |
| **MACROSECTOR** | `macrosector` | Texto | Sector al que pertenece la sociedad. |
| **INGRESOS OPERACIONALES**| `ingresos_operacionales`| Texto* | Ingresos operacionales registrados. |

*\*Nota: Los campos marcados como "Texto" son transformados a numéricos en el proceso de ETL.* <br>

<a name="directorio"></a>
## 📁 Directorio
```
Colombia-status-corporate-10k/
├── dashboard/
│   ├── capturas/             # Screenshots del tablero interactivo
│   └── reporte_final.pbix    # Archivo fuente de Power BI
├── docs/
│   └── insights.md           # Análisis de los datos
├── sql/
│   └── queries_negocio.sql   # Consultas estratégicas (Análisis de datos)
├── src/
│   └── etl_pipeline.py       # Script principal: API → Limpieza → Modelo Estrella → MySQL
├── .gitignore                # Exclusión de archivos temporales y locales
├── README.md                 # Documentación principal del proyecto
└── requirements.txt          # Dependencias necesarias
```  

<a name="proposito"></a>
## 💡 Propósito del proyecto
El objetivo específico del proyecto se basó en diseñar una arquitectura ETL robusta que extrajo los datos crudos en formato JSON. Además, se creó un esquema estrella que normalizó los datos para respetar su integridad, eliminar redundancias y optimizar mejor las consultas con el fin de generar información accionable que permita tomar decisiones acertadas a las partes interesadas. El resultado final es un ecosistema automatizado que permite visualizar el panorama empresarial colombiano y responder preguntas estratégicas como:

  🧮 ¿Qué empresas han tenido un crecimiento positivo en su ganancia durante todos los años registrados? <br>
  🥇 En cada ciudad, ¿qué porcentaje de los ingresos totales de su sector captura la empresa líder? <br>
  📊 ¿En qué departamentos de Colombia es más estratégico invertir según el macrosector económico?
   
<a name="arquitectura"></a>
## 🏗️ Arquitectura de Datos
El proyecto fue construido bajo un pipeline end-to-end automatizado que extrae los datos financieros más recientes de las empresas alojados en un servidor y compartidos a través de datos.gov.co. Se realiza la petición para consumo de datos y los devuelve a través de la API  en formato JSON.
<img width="1315" height="445" alt="Gemini_Generated_Image_o3l8eho3l8eho3l8" src="https://github.com/user-attachments/assets/81140b3f-91b5-4ff8-b279-23f85ba19a55" />

El viaje del dato:

**1. Ingesta(Extract)**: Se consume la API de Socrata de la web datos.gov.co mediante la librería Requests de Python garantizando la extracción total de 40.000 registros. <br>
A partir de ahí, los datos se almacenan en una tabla estructurada gracias a la conversión de datos en formato JSON a Dataframe que ofrece la librería Pandas de Python. <br>

**2. Procesamiento y modelado**: 
1. Se castean los datos a tipo númerico para el caso de columnas con cifras. También se eliminan duplicados y se estandarizan las columnas tipo texto.
2. Se realiza ingeniería de características mediante la creación de columnas que evalúan rendimientos financieros, entre ellas:
 - Margen neto
 - Índice de endeudamiento
 - ROA
 - ROE
 - Multiplicador del capital
<br>

- **Deduplicación de datos**: <br>
Limpieza y Deduplicación de Datos Para garantizar la calidad de la base de datos de empresas, se implementó un proceso de **Fuzzy String Matching** utilizando la librería PolyFuzz. Esto permitió identificar y agrupar variaciones de nombres de empresas (ej. "Empresa S.A." vs "Empresa SA") que el ojo humano detecta pero que un sistema tradicional vería como registros diferentes.<br>

```
datos_originales=empresas['nombre_limpio'].unique().tolist()
rapid_fuzz=RapidFuzz(n_jobs=-1)
model=PolyFuzz(rapid_fuzz)
model.match(datos_originales,datos_originales)
model.group(link_min_similarity=0.94)
clusters=model.get_clusters()
```

Posteriormente se crea un modelo de datos mediante un **esquema estrella** con cinco tablas dimensiones y una tabla de hechos que contiene columnas numéricas y claves foráneas. <br>
Se exportan las tablas al data warehouse MySQL a través del motor de SQLAlchemy. <br>

<p align="center">
  <<img width="1408" height="768" alt="Gemini_Generated_Image_hjupyahjupyahjup" src="https://github.com/user-attachments/assets/cf94d18f-e5b7-426f-b322-916cd9ebd898" /> />
</p>

**3. Data warehouse**: Luego del data cleansing, se almacenan los datos en la base de datos de MySQL permitiendo realizar 9 consultas relevantes para el análisis exploratorio de los datos y probar la eficacia del modelo de datos previo a la exportación de las tablas a Power BI. Puedes ver el análisis en la base de datos MySQL [aquí](docs/insights_sql.md).















