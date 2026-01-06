## 📈 Análisis financiero de las 10.000 empresas más grande de Colombia 
Aquí se detallan los resultados obtenidos en base a las consultas realizadas en el motor de MySQL para el conjunto de datos en cuestión. Véase el código fuente [aquí](/sql/consultas.sql) <br>

### 1. ¿Cuáles son los 3 sectores en cada departamento que ofrecen el mayor ROE promedio?
**Hallazgos claves**<br>
a. Se identificó que el sector servicios(Transporte, turismo, servicios de salud y estéticos, tecnología y servicios informáticos, entre otros) posee la mayor rentabilidad en las principales ciudades capitales del país con un promedio del 11%, lo cual representa los principales destinos para inversión en esta actividad. <br>
b. Bolívar y la minería representan el ROE más alto de la lista con un 53%. Esto indica una alta eficacia operativa y un punto de interés inmediato para inversionistas.<br>
c. Desierto de datos: Esto implica que los territorios donde hay muy pocos registros, exista una monodependencia. Si el sector líder (Servicios) cae, no hay otros sectores que estén generando rentabilidad real, lo que representa un riesgo de seguridad económica regional. Esto permite al gobierno nacional o la banca privada ejecutar inversiones para apoyar sectores económicos diversos con el fin de sostener la economía local con generación de empleo. <br>

| Macrosector        | Departamento         | Rentabilidad (ROE %) | Clasificación        |
|--------------------|----------------------|----------------------|----------------------|
| ⛏️ Minero          | Bolívar              | 53.33%               | 🚀 Máximo Retorno    |
| 🧩 Servicios       | La Guajira            | 50.00%               | 🚀 Máximo Retorno    |
| ⛏️ Minero          | Norte de Santander    | 42.86%               | 💎 Muy Alta          |
| 🧩 Servicios       | Amazonas              | 31.94%               | 💎 Muy Alta          |
| 🏭 Manufactura     | Arauca               | 22.22%               | 🟢 Alta              |
|--------------------|----------------------|----------------------|----------------------|
| 🧩 Servicios       | Cesar                | 5.14%                | ⚠️ Margen Bajo       |
| 🏗️ Construcción   | San Andrés            | 4.46%                | ⚠️ Margen Bajo       |
| 🛒 Comercio        | Arauca               | 3.33%                | 📉 Supervivencia     |
| 🛒 Comercio        | Nariño               | 3.21%                | 📉 Supervivencia     |
| 🛒 Comercio        | San Andrés            | 1.44%                | 📉 Supervivencia     |


*\*Nota: Ejemplo del resultado de la consulta realizada* <br>

### 2. ¿Qué empresas que han tenido un crecimiento positivo en su ganancia_perdida durante todos los años registrados?
La consulta identificó 30 empresas con crecimiento en sus utilidades durante los años 2021-2024. En un mercado volátil como el colombiano, estas organizaciones demuestran eficiencia operativa y traslado de costos al cliente sin perder margen. <br>
**Aplicación estratégica**: Para inversionistas de capital de riesgo interesados en el mercado colombiano, esta consulta puede ofrecerles una mirada específica hacia estas empresas económicamente sostenibles con el propósito de diversificar su capital. <br>

| Automotores Toyota Colombia SAS | Bel Star SA | Cfc Gas Holding SAS | Colombia Movil SA E S P | Comercial Nutresa SAS | Distribuidora Colombina Limitada |
|--------------------------------|-------------|--------------------|------------------------|----------------------|----------------------------------|
| Empresa Colombiana De Cementos SAS | Grupo Argos SA | Grupo Bolivar S A | Gyplac SA | Hoteles Estelar SA | Industria Colombiana De Motocicletas Yamaha SA |
| Industrias Electromecanicas Magnetron SAS | Interconexión Eléctrica SA ESP - Isa | Kopps Commercial SAS | Macfer Sca | Mc Victorias Tempranas SAS | Miniso Colombia SAS |
| Oleoducto Central SA | Organizacion Delima SA Chc | Pintuco Colombia SAS | Plural Comunicaciones SAS | Productos Familia SA | Rappi SAS |
| Samaria Llanos Exploration Sucursal Colombia | Soluciones Bolivar SAS | Sporty City SAS | Suramerica Comercial SAS | Torrecafe Aguila Roja Y Compañia SA | Wework Colombia SAS |
<br>

### 3. ¿Qué empresas tienen Patrimonio negativo a lo largo de los años entre 2021-2024?
Este análisis identifica a las empresas cuyo patrimonio ha decrecido a niveles negativos durante el marco temporal entre 2021-2024.
<br>
**Por qué es crítico para inversionistas y el Estado:**
<br>
**Riesgo de Continuidad**: Estas empresas operan bajo una estructura de "quiebra técnica". Para un inversionista, representan un riesgo total de pérdida de capital.<br>
**Intervención regulatoria**: La Supersociedades debe encargarse de la reorganización empresarial de estas empresas. Algunas de ellas ya están en estos trámites para hacer efectiva su liquidación. <br>
A pesar de estar inscritas en esta lista de las 10.000 empresas con mayores ingresos o capital reportado, es muy probable que se mantengan operativas mediante la dependencia crítica de endeudamiento o de inyecciones constantes de capital de sus matrices para mantenerse activas. <br>

| Acerias Paz De Rio SA | Almacenes Flamingo SA | Andean Tower Partners Colombia SAS | Bbi Colombia SAS | Bigfoot Colombia SAS | Cementos Tequendama SAS |
|----------------------|----------------------|-----------------------------------|-----------------|----------------------|-------------------------|
| Co Internet SAS | Colombia Telecomunicaciones SA ESP - Telefonica | Compañia Nacional De Levaduras Levapan SA | Compunet SA | Construcciones El Condor SA | Constructora Conconcreto SA |
| Directv Colombia Limitada | Durman Colombia SAS | Emerald Energy Plc Sucursal Colombia | Experts Colombia SAS | Fabricato SA | Ferrovial Construccion SA Sucursal Colombia |
| Ford Motor Colombia SAS | Fundacion Delamujer Colombia SAS | Garces Eder SAS |  |  |  | <br>

*\*Nota: Muestra del total de empresas en quiebra técnica* <br>

### 4.Calcular el índice de endeudamiento promedio por departamento

El índice de endeudamiento (Total Pasivos/Total Activos) mide qué tanta parte de la empresa pertenece a terceros (bancos, proveedores, impuestos) en lugar de a los dueños. Al promediarlo por departamento, descubrí qué regiones tienen economías más arriesgadas y cuáles son más sólidas al categorizarlos de la siguiente manera:
<br>
🔴 Riesgo Alto (>70%): Departamentos con alta dependencia del crédito. Ante una subida de tasas de interés, estas regiones son las primeras en entrar en crisis.<br>
🟡 Riesgo Medio (40-70%): Estructuras de capital equilibradas, comunes en sectores de manufactura y comercio.<br>
🟢 Riesgo Bajo (<40%): Regiones con empresas muy sólidas o que se autofinancian. Tienen mayor "colchón" para resistir épocas de vacas flacas.
<br>
En general, la mayoría de las departamento poseen un nivel Medio. Pese a esto, ientras que regiones como el Amazonas, Vichada y San Andrés muestran un apalancamiento conservador (riesgo bajo), Vaupés y Montería representan empresas con fragilidad estructural para llevar a cabo sus operaciones (riesgo alto).
<br>
| Departamento | Endeudamiento Promedio (%) | Clasificación de Riesgo |
|-------------|----------------------------|--------------------------|
| Vaupés | 100.00% | 🔴 Riesgo Alto |
| Montería | 75.00% | 🔴 Riesgo Alto |
| --- | --- | --- |
| Guaviare | 61.76% | 🟡 Medio |
| Atlántico | 59.92% | 🟡 Medio |
| Sucre | 59.06% | 🟡 Medio |
| Caquetá | 58.73% | 🟡 Medio |
| Cundinamarca | 57.18% | 🟡 Medio |
| Bogotá DC | 56.85% | 🟡 Medio |
| Risaralda | 55.64% | 🟡 Medio |
| --- | --- | --- |
| San Andrés y Providencia | 36.40% | 🟢 Riesgo Bajo |
| Amazonas | 31.55% | 🟢 Riesgo Bajo |
| Vichada | 22.22% | 🟢 Riesgo Bajo |
<br>
### 5. (Venture capital) Encontrar las empresas cuyos ingresos crecieron por encima del percentil 95 en su respectivo macrosector (outliers)
Esta consulta filtra las empresas que superan el percentil 95 de su propio macrosector al cierre del año 2024. Estas empresas crecieron significativamente más que sus competidores, lo que las categoriza como empresas outliers de alto rendimiento, a menudo por modelos de negocio innovadores o ventajas tecnológicas. 

| Industria | Rk | Empresa                               | Tasa (%) de crecimiento |
|------|----|--------------------------------------|----------|
| 🥑 Agr | 1  | Camposol Colombia SAS                 | 200.0    |
| 🥑 Agr | 2  | Sociedad Comercialización Calafate SAS| 112.5    |
| 🥑 Agr | 3  | Bananeras Agrofuturo SAS              | 100.0    |
| 🛒 Com | 1  | CI Golden Agri-Resources Colombia SAS | 600.0    |
| 🛒 Com | 2  | Prolife Biotech Colombia SAS          | 425.0    |
| 🛒 Com | 3  | Sociedad Comercialización Naranja    | 400.0    |
| 🏗️ Inf | 1  | China Harbour Engineering Colombia    | 580.0    |
| 🏗️ Inf | 2  | Byb Constructores SAS                 | 350.0    |
| 🏗️ Inf | 3  | Concesionaria Vial Del Pacifico SAS  | 315.38   |
| 🏭 Man | 1  | Odin Petroil SA                       | 344.44   |
| 🏭 Man | 2  | Alambres Y Cables Técnicos SAS        | 250.0    |
| 🏭 Man | 3  | Panamericana De Alimentos SAS         | 216.67   |
| ⛏️ Min | 1  | Puerto Arturo SAS                     | 400.0    |
| ⛏️ Min | 2  | Mkms Enerji Colombia                   | 220.0    |
| ⛏️ Min | 3  | Promisol SAS                           | 83.33    |
| 🧩 Ser | 1  | Micro Inversiones SAS                  | 1900.0   |
| 🧩 Ser | 2  | Greenyellow Energia Colombia SAS       | 1000.0   |
| 🧩 Ser | 3  | Latin Logistic Colombia SAS            | 850.0    | 


*\*Nota: Top 3 de empresas outliers por macrosector* <br>

**El Fenómeno de Servicios**: Es impactante observar que en el sector Servicios, el crecimiento llega hasta un 1900% (Micro Inversiones SAS), lo cual es típico de modelos de base tecnológica. <br>

**Construcción e Infraestructura**: El liderazgo de China Harbour Engineering con un 580% refleja la ejecución de grandes proyectos de infraestructura (4G/5G) en el país, un dato clave para el análisis del PIB nacional. <br>

**Outliers vs. Media**: Mientras que el crecimiento promedio de la economía puede estar entre el 1% y 10%, estas empresas operan en una realidad distinta, superando el 100% de crecimiento anual en casi todos los sectores. <br>

### 6. Detección de "Empresas Estrella" (Matriz BCG - Inversión)
La matriz BCG es un cuadrante que permite clasificar a las empresas según el crecimiento dentro de su sector y qué tan fuerte es frente a sus competidores.
| Categoría | Estrategia   | Hallazgo Clave |
|:---------:|:------------:|----------------|
| ⭐ Estrellas | Invertir | Empresas en sectores de alto crecimiento con dominancia de mercado. |
| 🐄 Vacas | Ordeñar | Líderes en sectores maduros; generan el flujo de caja que sostiene la economía. |
| ❓ Interrogantes | Analizar | Empresas en sectores explosivos pero con baja cuota; alto potencial de ser Estrellas. |
| 🐕 Perros | Desinvertir | Empresas con bajo crecimiento y baja cuota; alto riesgo de estancamiento. |

El análisis revela que el sector servicios concentra la mayor cantidad de 'estrellas', mientras que el sector manufactura domina en empresas se desplaza hacia el cuadrante de las vacas, indicando empresas maduras y con una sólida economía. Por el otro lado están los 'interrogantes' donde continúan dominando las empresas de servicios, esto puede darse por escenarios donde se crean startups gracias al avance de la IA pero que aún no relfjan un dominio claro en su nicho. Por último están las empresas perro donde el comercio domina por su bajo crecimiento y/o estancamiento.<br>
Estos datos reflejan que para cualquier inversor o emprendedor, es importante conocer de antemano el nicho en el cual desea desarrollar su idea o capital, pues el panorama actual en Colombia ofrece una diversificación de empresas que impulsan la economía, pero hay que saber encontrarla.

### 7. (Dominancia del mercado) En cada ciudad, ¿qué porcentaje de los ingresos totales de su sector captura la empresa líder?
En esta consulta se refleja la dominancia de cada empresa en su respectivo sector. Esto es importante porque verifica si existen monopolios y fijación de precios por parte unas pocas firmas. A continuación se crea una tabla utilizada para el propósito de identificar dominancia de mercado:

**📏 Escala de Dominancia de Mercado**

| Porcentaje de Captura | Clasificación Técnica   | Significado Económico |
|----------------------|-------------------------|-----------------------|
| > 90%               | Monopolio Puro          | Una sola empresa controla virtualmente toda la oferta. No hay competencia real. |
| 70% - 90%           | Monopolio de Hecho      | Dominancia absoluta. La empresa tiene poder total para fijar precios. |
| 40% - 70%           | Posición de Dominio     | Mercado altamente concentrado. La empresa actúa como líder de precios. |
| 20% - 40%           | Oligopolio Fuerte       | Pocos jugadores grandes. Hay competencia, pero el líder tiene mucha influencia. |
| < 15%               | Mercado Atomizado       | Competencia perfecta o mercado fragmentado. Ninguna empresa dicta las reglas. |

Con base en esto se filtra las empresas con dominancia mayor al 50% de captación de ingresos. En algunos departamentos existen monopolios puros:

| Empresa                     | Departamento | Sector        |
|----------------------------|--------------|---------------|
| Oceanos SA                 | Bolívar      | Agropecuario  |
| Aris Mining Marmato        | Caldas       | Minero        |
| Agrícola del Occidente     | Cauca        | Agropecuario  |
| Juanthosa Red              | Vaupés       | Servicios     |
| Riopaila Palma             | Vichada      | Agropecuario  |

Estas son empresas que fijan precios en sus respectivos sectores y territorios. Son mercados de un solo jugador.
<br>
A su vez, estas empresas representan posiciones de dominio elevado, no son las únicas, pero capturan el mayor porcentaje en sus zonas:

| Empresa                 | Departamento | Sector        | Captura del Sector |
|-------------------------|--------------|---------------|--------------------|
| Ecopetrol               | Bogotá       | Minero        | 63%                |
| Refinería de Cartagena  | Bolívar      | Manufactura   | 65%                |
| Tecnioriente Energy     | Arauca       | Manufactura   | 83%                |

En general, muchos sectores en diferentes zonas se encuentran en un mercado atomizado, esto indica una sana competencia. No obstante, los inversionistas deben capturar esta data con el objetivo de analizar a profundidad qué sectores y territorios tienen bajo monopolio con miras a un crecimiento sostenido que posicione a la empresa en puestos de vanguardia.<br>

### 8. Rotación de activos totales ¿Qué tan productiva es la infraestructura de cada departamento en Colombia en el año 2024?
la Rotación de Activos (Ingresos/Activos) nos dice cuántos pesos de venta genera la empresa por cada peso que tiene invertido en infraestructura, maquinaria o inventarios.

| Nivel de Eficiencia | Ratio            | Interpretación |
|--------------------|------------------|----------------|
| Alta               | > 1              | Generan más de lo que poseen en activos. |
| Media              | 0.4 – 1          | Equilibrio industrial. |
| Baja               | < 0.4            | Alta intensidad de capital o activos subutilizados. |

Dentro de este ratio se hallan algunas conclusiones relevantes: <br>
1. Los departamentos con mayor productividad en una gran proporción no alojan ciudades capitales. Esto, en conjunto con los resultados evidenciados en la pregunta 7 reflejan alto monopolio y muy poca presencia de competidores fuertes, lo cual hace a estas empresas ubicados aquí muy rentables.<br>
2. Los deptos con ciudades capitales como Cundinamarca, Antioquia, El Atlántico y el Valle tienen productividad media, debido a una mayor presencia de organizaciones.
3. Solo existe un dpto con baja productividad (Vichada). Es importante que el gobierno nacional establezca mecanismos que impulsen el sector económico de esta región.

### 9. (Mapa de competitividad) Con base en la rotación de activos y la matriz BCG, ¿en qué departamentos de Colombia es más estratégico invertir según el macrosector económico
La inversión estratégica ocurre donde la productividad de la infraestructura es alta y el posicionamiento de mercado es dominante (Estrellas). Este análisis permite recomendar ubicaciones específicas para nuevos proyectos de expansión o inversión de capital basado en el año 2024.<br>

Esta consulta especifica cuáles son los departamentos de Colombia que obtienen mayores ingresos con sus activos en cada uno de los sectores del estudio.<br>

| Macrosector        | Departamento | Rotación de Activos | Clasificación              |
|--------------------|--------------|---------------------|----------------------------|
| 🛒 Comercio        | Chocó        | 8.50                | 🚀 Máxima Productividad    |
| 🛒 Comercio        | Casanare     | 4.00                | 🟢 Alta productividad      |
| 🏭 Manufactura    | Caquetá      | 3.00                | 🟢 Alta productividad      |
| 🧩 Servicios      | Vaupés       | 3.00                | 🟢 Alta productividad      |
| ⛏️ Minero         | Bolívar      | 2.29                | 🟢 Alta productividad      |
| 🥑 Agropecuario   | Nariño       | 2.25                | 🟢 Alta productividad      |
| 🛒 Comercio        | Bogotá DC    | 1.52                | 🟢 Alta productividad      |
| 🏭 Manufactura    | Antioquia    | 1.09                | 🟢 Alta productividad      |
| 🏗️ Construcción   | Bogotá DC    | 0.32                | 🔴 Baja productividad      |
| 🧩 Servicios      | Antioquia    | 0.32                | 🔴 Baja productividad      |

*\*Nota: Esta es una muestra del total de departamentos por el total de macrosectores* <br>

**Análisis de Competitividad Estratégica**

Al analizar estos 10,000 registros de las principales empresas para el año 2024 , surgen conclusiones que desafían la intuición común:
1. El Fenómeno del Comercio en la Periferia: El Chocó lidera la eficiencia en Comercio (8.5). Esto indica que en zonas con poca infraestructura, el capital fluye con extrema rapidez; las empresas poseen muy pocos activos fijos (locales, bodegas) pero mueven inventarios constantemente.
2. Construcción: El peso de las capitales: Es revelador que en Bogotá (0.3) y Antioquia (0.35) la construcción tenga "Baja productividad". Esto se debe a que en las grandes ciudades los activos (terrenos y maquinaria pesada) son extremadamente costosos, lo que "infla" el denominador y baja el ratio de rotación.
3. Manufactura Sólida: Antioquia y Bogotá logran mantenerse en "Alta productividad" (>1.0) a pesar de ser industrias pesadas. Esto demuestra una madurez operativa: logran vender más de lo que tienen invertido en planta y equipo.
4. Servicios en Regiones Emergentes: Departamentos como Vaupés (3.0) y Arauca (2.0) son "joyas ocultas" para servicios. Al ser negocios basados en talento humano y no en maquinaria, su retorno sobre activos es explosivo.




