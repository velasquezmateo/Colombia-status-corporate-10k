## 📈 Análisis financiero de las 10.000 empresas más grande de Colombia 
Aquí se detallan los resultados obtenidos en base a las consultas realizadas en el motor de MySQL para el conjunto de datos en cuestión. <br>

### 1. ¿Cuáles son los 3 sectores en cada departamento que ofrecen el mayor ROE promedio?
**Hallazgos claves**<br>
- Se identificó que el sector servicios(Transporte, turismo, servicios de salud y estéticos, tecnología y servicios informáticos, entre otros) posee la mayor rentabilidad en las principales ciudades capitales del país con un promedio del 11%, lo cual representa los principales destinos para inversión en esta actividad. <br>
- Bolívar y la minería representan el ROE más alto de la lista con un 53%. Esto indica una alta eficacia operativa y un punto de interés inmediato para inversionistas.<br>
- Desierto de datos: Esto implica que los territorios donde hay muy pocos registros, exista una monodependencia. Si el sector líder (Servicios) cae, no hay otros sectores que estén generando rentabilidad real, lo que representa un riesgo de seguridad económica regional. Esto permite al gobierno nacional o la banca privada ejecutar inversiones para apoyar sectores económicos diversos con el fin de sostener la economía local con generación de empleo. <br>
<img width="390" height="158" alt="imagen" src="https://github.com/user-attachments/assets/44ccb144-e41c-40f1-af2c-aabf938f538a" /> <br>
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
<br>
| Empresa | Macrosector | Endeudamiento (%) | Año |
|--------|-------------|------------------|-----|
| Camposol Colombia SAS | Agropecuario | 200.00 | 2024 |
| Sociedad De Comercializacion Internacional Calafate SAS | Agropecuario | 112.50 | 2024 |
| Bananeras Agrofuturo SAS | Agropecuario | 100.00 | 2024 |
| CI Golden Agri-Resources Colombia SAS | Comercio | 600.00 | 2024 |
| Prolife Biotech Colombia SAS | Comercio | 425.00 | 2024 |
| Sociedad De Comercializacion Internacional De Negocios Naranja Y Tecnologia Financiera SAS Bic | Comercio | 400.00 | 2024 |
| China Harbour Engineering Company Limited Colombia | Construcción | 580.00 | 2024 |
| Byb Constructores SAS | Construcción | 350.00 | 2024 |
| Concesionaria Vial Del Pacifico SAS | Construcción | 315.38 | 2024 |
| Odin Petroil SA En Reestructuracion | Manufactura | 344.44 | 2024 |
| Alambres Y Cables Tecnicos SAS | Manufactura | 250.00 | 2024 |
| Panamericana De Alimentos SAS | Manufactura | 216.67 | 2024 |
| Puerto Arturo SAS | Minero | 400.00 | 2024 |
| Mkms Enerji Sucursal Colombia | Minero | 220.00 | 2024 |
| Promisol SAS | Minero | 83.33 | 2024 |
| Micro Inversiones SAS | Servicios | 1900.00 | 2024 |
| Greenyellow Energia De Colombia SAS | Servicios | 1000.00 | 2024 |
| Latin Logistic Colombia SAS | Servicios | 850.00 | 2024 | <br>
*\*Nota: Top 3 de empresas outliers por macrosector* <br>


