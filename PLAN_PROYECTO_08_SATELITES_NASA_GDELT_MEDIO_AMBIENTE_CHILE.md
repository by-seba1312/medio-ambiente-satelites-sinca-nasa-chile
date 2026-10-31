# PLAN DE EJECUCIÓN DETERMINISTA (OPTIMIZADO PARA GEMINI FLASH HIGH) — PROYECTO 8: Joya Oculta #10 (Focos Satelitales NASA FIRMS MODIS/VIIRS + Eventos Globales GDELT Chile Zero-Copy en BigQuery) y Ministerio del Medio Ambiente (SINCA Calidad del Aire en Línea, RETC Emisiones/Residuos, SMA Sanciones SNIFA y Áreas Protegidas SBAP)

> **Modelo Objetivo de Ejecución:** `Gemini Flash (High)` — Instrucciones paso a paso 100% deterministas, con sentencias SQL DDL `CREATE TABLE IF NOT EXISTS` y Vistas Federadas Zero-Copy listas para ejecutar.  
> **Proyecto GCP:** `project-0ab8074c-14aa-434e-849`  
> **Región BigQuery y Cloud Run:** `southamerica-west1` (Santiago de Chile)  
> **Dataset Destino en BigQuery:** `core_medio_ambiente_satelital` *(Dataset Nuevo con `storage_billing_model = 'PHYSICAL'`)* + integración con `core_riesgos_emergencias`  
> **Vistas Gold a Crear en `gold_nacional`:**  
>   1. `vw_monitoreo_satelital_incendios_nasa_firms_y_calidad_aire_sinca`  
>   2. `vw_pulso_eventos_sociales_politicos_gdelt_y_noticias_chile` *(Zero-Copy sobre `gdelt-bq.gdeltv2` + `fact_noticias_mercados_chile`)*  
> **Repositorio GitHub a Crear/Actualizar:** `by-seba1312/medio-ambiente-satelites-sinca-nasa-chile` + `by-seba1312/data-lakehouse-nacional-chile`  
> **Carpeta Local Google Drive:** `g:\Mi unidad\Datos Publicos\08_Satelites_NASA_FIRMS_GDELT_y_Medio_Ambiente`  

---

## ⚠️ REGLAS TÉCNICAS CRÍTICAS PARA EL AGENTE (GEMINI FLASH HIGH)

1. **Parámetro Exacto en BigQuery MCP (`execute_sql` y `execute_sql_readonly`):**
   - Usa **SIEMPRE** `projectId: "project-0ab8074c-14aa-434e-849"` (en camelCase). **NUNCA** uses `project_id` con guion bajo.
2. **Palabras Reservadas y Vistas de Sistema en BigQuery:**
   - **NUNCA** uses `AS rows` como alias en SQL (usa `AS total_filas`).
   - Para auditar tamaños de tablas usa `<dataset>.INFORMATION_SCHEMA.PARTITIONS`.
3. **Regla de Oro de Trazabilidad (Obligatoria al cerrar cada Hito):**
   > *"actualiza el repo en github de los hitos. deja las fuentes con sus enlaces muy precisos y revisa y recontra revisa que los datos ingresados sean correctos y los enlaces funcionen. Y carga ordenadamente a google drive todos los archivos que descargaste y usaste para la ingesta de datos a google cloud. todo esto para trazabilidad. ademas actualiza el spiderweb en google cloud para google cloud run y google cloud scheduler. deja esta frase actualizada en el repo para trazabilidad"*

---

## 🏛️ LOS 5 PRINCIPIOS OBLIGATORIOS DE MÁXIMO APROVECHAMIENTO DE CAPACIDAD DE ALMACENAMIENTO

1. **Crear el Dataset Directamente con Facturación Física Comprimida (`storage_billing_model = 'PHYSICAL'`):**
   - El dataset `core_medio_ambiente_satelital` nace con `OPTIONS(location = 'southamerica-west1', storage_billing_model = 'PHYSICAL')`.
2. **Cero Tablas Duplicadas (Reutilización Zero-Copy):**
   - **PROHIBIDO duplicar** los proyectos de inversión ambiental e-SEIA (`core_geologia_mineria.fact_seia_proyectos_inversion_gis`), incendios administrativos CONAF (`core_riesgos_emergencias.fact_incendios_conaf`) o estaciones meteorológicas INIA (`agro_chile.lecturas_clima_diario`). Se cruzan mediante `JOIN` espacial (`ST_DWITHIN`) o por `cut_comuna`.
   - Para **GDELT Project**, se extrae y actualiza únicamente el subconjunto filtrado de Chile (`ActionGeo_CountryCode = 'CI'`) optimizado en `fact_gdelt_eventos_sociales_politicos_chile`, evitando copiar datos fuera de Chile.
3. **Prohibición de Strings Repetidos en Tablas Masivas (Codificación por Diccionario `INT64` / Códigos Cortos):**
   - En lecturas de calidad del aire SINCA se almacena solo `id_estacion_sinca INT64`, `parametro_cod STRING` (`MP25`, `MP10`, `O3`, `SO2`, `NO2`, `CO`) y `valor_ug_m3 FLOAT64`; los nombres y coordenadas de estaciones viven **una sola vez** en `dim_estaciones_sinca_y_areas_protegidas_gis`.
4. **Particionamiento + `CLUSTER BY` Multi-Columna:**
   - Tablas particionadas por `fecha` / `anio` y agrupadas físicamente por `CLUSTER BY cut_comuna, parametro_cod` (o `satelite_instrumento`).
5. **Archivos Crudos en Google Drive y Cloud Storage en Formato `Parquet + ZSTD (Nivel 19)`:**
   - Todo archivo descargado se convierte automáticamente a **Apache Parquet con compresión `ZSTD` nivel 19** (`df.to_parquet(..., compression='zstd', compression_level=19)`) en `g:\Mi unidad\Datos Publicos\08_Satelites_NASA_FIRMS_GDELT_y_Medio_Ambiente\raw_parquet\` y en `gs://chile-macro-bronze-0ab8074c/medio_ambiente_satelital/`.

---

## PASO 0 (EJECUTAR PRIMERO): Crear el Dataset `core_medio_ambiente_satelital` y sus 6 Tablas en BigQuery

Ejecuta mediante `call_mcp_tool` (`ServerName: "bigquery"`, `ToolName: "execute_sql"`, `projectId: "project-0ab8074c-14aa-434e-849"`):

```sql
CREATE SCHEMA IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital`
OPTIONS (
  location = 'southamerica-west1',
  storage_billing_model = 'PHYSICAL',
  description = 'Data Lakehouse Silver: Monitoreo Satelital NASA FIRMS (MODIS/VIIRS), Eventos Globales GDELT Chile y Ministerio del Medio Ambiente (SINCA, RETC, SMA SNIFA, SBAP)'
);

-- 1. JOYA OCULTA #10 (Parte A): Focos de Calor y Anomalías Térmicas Satelitales NASA FIRMS (MODIS / VIIRS Suomi-NPP y NOAA-20/21)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_nasa_firms_focos_calor_satelital_gis` (
  id_foco_satelital STRING NOT NULL,
  fecha_captura DATE NOT NULL,
  fecha_hora_utc TIMESTAMP NOT NULL,
  satelite_instrumento STRING,         -- VIIRS_SNPP, VIIRS_NOAA20, VIIRS_NOAA21, MODIS_TERRA_AQUA
  latitud FLOAT64 NOT NULL,
  longitud FLOAT64 NOT NULL,
  geometria_punto GEOGRAPHY,
  brillo_termico_kelvin FLOAT64,
  potencia_radiativa_fuego_frp_mw FLOAT64, -- Fire Radiative Power (MW)
  nivel_confianza STRING,              -- HIGH, NOMINAL, LOW
  dia_o_noche STRING,                  -- D, N
  codigo_region INT64,
  cut_comuna STRING,
  nombre_comuna STRING,
  distancia_a_viviendas_censo_m FLOAT64,
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY fecha_captura
CLUSTER BY cut_comuna, satelite_instrumento, nivel_confianza;

-- 2. JOYA OCULTA #10 (Parte B): Eventos Políticos, Sociales, Protestas y Conflictos en Chile (GDELT Project Global Knowledge Graph & Events)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_gdelt_eventos_sociales_politicos_chile` (
  global_event_id INT64 NOT NULL,
  fecha_evento DATE NOT NULL,
  anio INT64 NOT NULL,
  actor1_nombre STRING,
  actor1_tipo_cameo STRING,            -- GOV, LEG, JUD, COP, BUS, LAB, EDU, MED, CVL
  actor2_nombre STRING,
  actor2_tipo_cameo STRING,
  event_root_code_cameo STRING,        -- 14=Protesta, 19=Uso de fuerza, 01..08=Cooperación/Acuerdo diplomático o legislativo
  glosa_tipo_evento STRING,
  quad_class INT64,                    -- 1=Cooperación Verbal, 2=Cooperación Material, 3=Conflicto Verbal, 4=Conflicto Material
  goldstein_scale_impacto FLOAT64,     -- -10.0 (Conflicto extremo) a +10.0 (Cooperación máxima)
  num_menciones_prensa INT64,
  num_articulos_fuente INT64,
  tono_promedio_sentimiento FLOAT64,   -- AvgTone GDELT
  ubicacion_nombre STRING,
  codigo_region INT64,
  cut_comuna STRING,
  latitud FLOAT64,
  longitud FLOAT64,
  geometria_punto GEOGRAPHY,
  source_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY fecha_evento
CLUSTER BY quad_class, event_root_code_cameo, cut_comuna;

-- 3. Dimensión Georreferenciada de Estaciones de Calidad del Aire SINCA, Humedales Urbanos y Áreas Protegidas (SNASPE / SBAP)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.dim_estaciones_sinca_y_areas_protegidas_gis` (
  id_elemento_ambiental STRING NOT NULL,
  tipo_elemento STRING NOT NULL,       -- ESTACION_SINCA_AIRE, PARQUE_NACIONAL_SNASPE, RESERVA_NACIONAL, SANTUARIO_NATURALEZA, HUMEDAL_URBANO_MMA
  nombre_elemento STRING,
  operador_o_administrador STRING,
  zona_saturada_o_latente_pda STRING,  -- Plan de Descontaminación Atmosférica (PDA) asociado
  superficie_ha FLOAT64,
  codigo_region INT64,
  cut_comuna STRING NOT NULL,
  nombre_comuna STRING,
  latitud FLOAT64,
  longitud FLOAT64,
  geometria GEOGRAPHY,
  estado_vigencia STRING,
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
CLUSTER BY tipo_elemento, cut_comuna;

-- 4. Hechos de Calidad del Aire SINCA (MP2.5, MP10, Ozono, SO2, NO2, CO y Episodios Críticos GEC)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_sinca_calidad_aire_y_episodios_criticos` (
  fecha_medicion DATE NOT NULL,
  anio INT64 NOT NULL,
  id_estacion_sinca STRING NOT NULL,
  cut_comuna STRING NOT NULL,
  parametro_cod STRING NOT NULL,       -- MP25, MP10, O3, SO2, NO2, CO
  concentracion_promedio_24h_ug_m3 FLOAT64,
  concentracion_max_horaria_ug_m3 FLOAT64,
  indice_calidad_aire_icap INT64,
  nivel_episodio_critico STRING,       -- BUENO, REGULAR, ALERTA_AMBIENTAL, PREEMERGENCIA, EMERGENCIA_AMBIENTAL
  supera_norma_primaria_bool BOOL,
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY fecha_medicion
CLUSTER BY cut_comuna, parametro_cod, id_estacion_sinca;

-- 5. Hechos del Registro de Emisiones y Transferencia de Contaminantes (RETC: Emisiones Aire/Agua y Residuos Peligrosos/Domiciliarios)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_mma_retc_emisiones_y_residuos_comunal` (
  anio_reporte INT64 NOT NULL,
  id_establecimiento_vu_retc INT64,
  rut_titular_num INT64,
  nombre_establecimiento STRING,
  rubro_ciiu4 STRING,
  codigo_region INT64,
  cut_comuna STRING NOT NULL,
  tipo_flujo_retc STRING NOT NULL,     -- EMISION_AIRE_FUENTE_FIJA, DESCARGA_AGUA_RILES, RESIDUO_PELIGROSO_SIDREP, RESIDUO_INDUSTRIAL_NO_PELIGROSO_SINADER, RESIDUO_DOMICILIARIO_MUNICIPAL
  contaminante_o_residuo STRING,       -- CO2, MP, SO2, NOx, Arsénico, Mercurio, Lodos, Aceites usados, RSU Municipal
  cantidad_ton_anual FLOAT64,
  destino_tratamiento STRING,          -- VALORIZACION_RECICLAJE, RELLENO_SANITARIO,VERTEDERO, INCINERACION, ELIMINACION_SEGURIDAD
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY RANGE_BUCKET(anio_reporte, GENERATE_ARRAY(2014, 2030, 1))
CLUSTER BY tipo_flujo_retc, cut_comuna;

-- 6. Hechos de Fiscalizaciones, Denuncias Ciudadanas y Sanciones Ambientales (SNIFA - Superintendencia del Medio Ambiente SMA)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_sma_snifa_fiscalizaciones_sanciones_y_denuncias` (
  anio INT64 NOT NULL,
  id_expediente_snifa STRING NOT NULL,
  tipo_proceso_sma STRING NOT NULL,    -- PROCEDIMIENTO_SANCIONATORIO, FISCALIZACION_AMBIENTAL, PROGRAMA_CUMPLIMIENTO_PDC, DENUNCIA_CIUDADANA, MEDIDA_PROVISIONAL
  unidad_fiscalizable_nombre STRING,
  categoria_economica STRING,          -- Minería, Energía, Acuicultura/Pesca, Agroindustria, Inmobiliario, Saneamiento, Forestal, Ruido
  rut_titular_num INT64,
  rut_titular STRING,
  codigo_region INT64,
  cut_comuna STRING NOT NULL,
  estado_procedimiento STRING,
  multa_aplicada_uta FLOAT64,
  monto_inversion_pdc_musd FLOAT64,
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY RANGE_BUCKET(anio, GENERATE_ARRAY(2013, 2030, 1))
CLUSTER BY tipo_proceso_sma, categoria_economica, cut_comuna;
```

---

## SECUENCIA DE HITOS DE INGESTA DE DATOS REALES (`core_medio_ambiente_satelital`)

### Hito MA1: Focos Satelitales NASA FIRMS (`fact_nasa_firms_focos_calor_satelital_gis`) + Eventos GDELT Chile (`fact_gdelt_eventos_sociales_politicos_chile`)
- **Fuentes Oficiales:**
  - NASA Fire Information for Resource Management System FIRMS (`https://firms.modaps.eosdis.nasa.gov/active_fire/` / API y feeds CSV MODIS y VIIRS para Sudamérica/Chile).
  - GDELT 2.0 Events Database (`https://www.gdeltproject.org` y dataset público en BigQuery `gdelt-bq.gdeltv2.events` filtrado por `ActionGeo_CountryCode = 'CI'`).

### Hito MA2: Estaciones SINCA, Humedales y Áreas Protegidas GIS (`dim_estaciones_sinca_y_areas_protegidas_gis`) + Calidad del Aire SINCA (`fact_sinca_calidad_aire_y_episodios_criticos`)
- **Fuentes Oficiales:**
  - Sistema de Información Nacional de Calidad del Aire SINCA — Ministerio del Medio Ambiente (`https://sinca.mma.gob.cl`).
  - Registro Nacional de Áreas Protegidas y Humedales Urbanos Declarados — MMA (`https://areasprotegidas.mma.gob.cl` y `https://humedaleschile.mma.gob.cl`).

### Hito MA3: Emisiones y Residuos RETC (`fact_mma_retc_emisiones_y_residuos_comunal`) + Fiscalizaciones y Sanciones SNIFA-SMA (`fact_sma_snifa_fiscalizaciones_sanciones_y_denuncias`)
- **Fuentes Oficiales:**
  - Portal de Datos Abiertos del Registro de Emisiones y Transferencia de Contaminantes RETC (`https://datosretc.mma.gob.cl`).
  - Sistema Nacional de Información de Fiscalización Ambiental SNIFA — Superintendencia del Medio Ambiente (`https://snifa.sma.gob.cl/DatosAbiertos`).

### Hito MA4: Creación de Vistas Gold, Noticias `MEDIO_AMBIENTE`, Spiderweb y Reporte de Costos Excel
1. Crear las 2 vistas en `gold_nacional`:
   - `gold_nacional.vw_monitoreo_satelital_incendios_nasa_firms_y_calidad_aire_sinca`
   - `gold_nacional.vw_pulso_eventos_sociales_politicos_gdelt_y_noticias_chile`
2. Ejecutar `python "g:\Mi unidad\Datos Publicos\generar_reporte_costos_diario_gcp.py"` para actualizar `REPORTE_COSTOS_DIARIOS_GCP_Y_ALMACENAMIENTO.xlsx`.
3. Subir todo al repositorio `by-seba1312/medio-ambiente-satelites-sinca-nasa-chile` y actualizar `by-seba1312/data-lakehouse-nacional-chile` dejando la frase oficial de trazabilidad.
