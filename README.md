# PROYECTO 8: Joya Oculta #10 — Focos Satelitales NASA FIRMS (MODIS/VIIRS) + Eventos Globales GDELT Chile Zero-Copy + Ministerio del Medio Ambiente (SINCA, RETC, SMA SNIFA y Áreas Protegidas SBAP)

> **Proyecto Google Cloud Platform:** `project-0ab8074c-14aa-434e-849`  
> **Región BigQuery:** `southamerica-west1` (Santiago de Chile)  
> **Dataset Silver Destino:** `core_medio_ambiente_satelital` *(Almacenamiento Físico `storage_billing_model = 'PHYSICAL'`)*  
> **Vistas Gold Multidominio:** `gold_nacional.vw_monitoreo_satelital_incendios_nasa_firms_y_calidad_aire_sinca` y `gold_nacional.vw_pulso_eventos_sociales_politicos_gdelt_y_noticias_chile`  
> **Repositorio Oficial:** `by-seba1312/medio-ambiente-satelites-sinca-nasa-chile`  
> **Data Lakehouse Central:** `by-seba1312/data-lakehouse-nacional-chile`  

---

## 🏛️ 1. Resumen Ejecutivo y Arquitectura

El Proyecto 8 integra las capacidades satelitales de teledetección térmica de incendios y anomalías térmicas en tiempo real de la NASA, con el análisis de eventos sociopolíticos globales de GDELT Project en Chile, y la infraestructura pública ambiental del Ministerio del Medio Ambiente (MMA) y la Superintendencia del Medio Ambiente (SMA).

### Cumplimiento de los 5 Principios de Máximo Aprovechamiento de Capacidad:
1. **Facturación de Almacenamiento Físico Directo (`PHYSICAL`):** El dataset `core_medio_ambiente_satelital` fue aprovisionado directamente con `OPTIONS(storage_billing_model = 'PHYSICAL')`, logrando reducciones de costo de almacenamiento de hasta un 75%.
2. **Cero Tablas Duplicadas:** Se enlazan y cruzan de forma Zero-Copy las comunas censales del INE (`ine_gold_gis.geo_comunas_censales`), las manzanas urbanas del Censo 2024 (`ine_gold_gis.geo_manzanas_censales_2024`) y los incendios CONAF (`core_riesgos_emergencias.fact_incendios_conaf`). Para GDELT, se extrajo exclusivamente la partición de Chile (`ActionGeo_CountryCode = 'CI'`).
3. **Codificación por Diccionario y Códigos Cortos:** Codificación normalizada con `id_estacion_sinca`, códigos de contaminantes (`MP25`, `MP10`, `O3`, `SO2`, `NO2`, `CO`) y códigos comunales `cut_comuna` (5 dígitos con padding).
4. **Particionamiento y Clustering Multi-Columna:** Las 6 tablas del dataset cuentan con esquemas de particionamiento diario por fecha o anual por rango y `CLUSTER BY` multi-columna optimizados para filtros de alta concurrencia.
5. **Preservación Cruda en Apache Parquet + ZSTD (Nivel 19):** Todos los datasets descargados fueron convertidos y almacenados en formato Apache Parquet con compresión ZSTD máxima (nivel 19) en la carpeta local de Google Drive y en el bucket de Cloud Storage `gs://chile-macro-bronze-0ab8074c/medio_ambiente_satelital/`.

---

## 📊 2. Auditoría de Tablas en BigQuery (`core_medio_ambiente_satelital`)

Auditoría oficial extraída directamente de `INFORMATION_SCHEMA.PARTITIONS`:

| # | Nombre de la Tabla | Descripción y Cobertura | Total Filas | Particionamiento | Clustering |
|---|---|---|---|---|---|
| 1 | `fact_nasa_firms_focos_calor_satelital_gis` | Focos satelitales MODIS (Terra/Aqua) y VIIRS (Suomi-NPP, NOAA-20, NOAA-21) en Chile con FRP (MW), brillo Kelvin y distancia calculada a viviendas Censo 2024 | **2.350** | `PARTITION BY fecha_captura` | `cut_comuna`, `satelite_instrumento`, `nivel_confianza` |
| 2 | `fact_gdelt_eventos_sociales_politicos_chile` | Eventos sociopolíticos, acuerdos, protestas y conflictos en Chile desde GDELT 2.0 con glosa CAMEO, escala Goldstein y sentimiento | **173.430** | `PARTITION BY fecha_evento` | `quad_class`, `event_root_code_cameo`, `cut_comuna` |
| 3 | `dim_estaciones_sinca_y_areas_protegidas_gis` | Catastro georreferenciado de 124 estaciones SINCA MMA, 20 Parques Nacionales SNASPE, Reservas Nacionales, Santuarios y 12 Humedales Urbanos Ley 21.202 | **166** | N/A (Dimensión) | `tipo_elemento`, `cut_comuna` |
| 4 | `fact_sinca_calidad_aire_y_episodios_criticos` | Mediciones de calidad del aire en 124 estaciones SINCA (MP2.5, MP10, O3, SO2, NO2, CO) con promedios 24h, ICAP y episodios críticos GEC | **66.960** | `PARTITION BY fecha_medicion` | `cut_comuna`, `parametro_cod`, `id_estacion_sinca` |
| 5 | `fact_mma_retc_emisiones_y_residuos_comunal` | Registro RETC: Emisiones al aire fuentes fijas, descargas RILES, residuos peligrosos SIDREP, industriales SINADER y RSU domiciliarios municipales | **1.840** | `PARTITION BY RANGE_BUCKET(anio_reporte)` | `tipo_flujo_retc`, `cut_comuna` |
| 6 | `fact_sma_snifa_fiscalizaciones_sanciones_y_denuncias` | Fiscalizaciones ambientales, expedientes sancionatorios, Programas de Cumplimiento (PdC MUSD), multas en UTA y denuncias ciudadanas SNIFA-SMA | **610** | `PARTITION BY RANGE_BUCKET(anio)` | `tipo_proceso_sma`, `categoria_economica`, `cut_comuna` |
| **TOTAL** | **Dataset `core_medio_ambiente_satelital`** | **Total consolidado en almacenamiento físico BigQuery** | **245.356** | — | — |

---

## 🌟 3. Vistas Gold Multidominio en `gold_nacional`

1. **`gold_nacional.vw_monitoreo_satelital_incendios_nasa_firms_y_calidad_aire_sinca`**:
   - Cruza focos satelitales NASA FIRMS (MODIS/VIIRS) con las estaciones de calidad del aire SINCA del Ministerio del Medio Ambiente, identificando comunas con anomalías térmicas activas, proximidad a viviendas censales Censo 2024, niveles críticos de MP2.5/MP10 y áreas silvestres protegidas / humedales urbanos en riesgo bajo Planes de Descontaminación Atmosférica (PDA).
2. **`gold_nacional.vw_pulso_eventos_sociales_politicos_gdelt_y_noticias_chile`**:
   - Monitoreo del clima sociopolítico nacional diario mediante la clasificación cuadrante CAMEO (Cooperación Verbal, Cooperación Material, Conflicto Verbal, Conflicto Material), eventos de protestas y marchas, escala de impacto Goldstein (-10 a +10), índice de sentimiento y cobertura mediática nacional e internacional geocodificada por comuna y región.

---

## 🔗 4. Fuentes Oficiales y Enlaces de Trazabilidad Directa

- **NASA FIRMS (Fire Information for Resource Management System):**
  - Portal Oficial: [https://firms.modaps.eosdis.nasa.gov](https://firms.modaps.eosdis.nasa.gov)
  - Feeds de Datos Activos CSV: [https://firms.modaps.eosdis.nasa.gov/active_fire/](https://firms.modaps.eosdis.nasa.gov/active_fire/)
  - VIIRS Suomi-NPP: `https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_South_America_7d.csv`
  - VIIRS NOAA-20: `https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_South_America_7d.csv`
  - VIIRS NOAA-21: `https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_South_America_7d.csv`
  - MODIS C6.1: `https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_South_America_7d.csv`
- **GDELT Project (Global Database of Events, Language, and Tone):**
  - Portal Oficial: [https://www.gdeltproject.org](https://www.gdeltproject.org)
  - Dataset Público BigQuery: `gdelt-bq.gdeltv2.events_partitioned`
- **SINCA — Sistema de Información Nacional de Calidad del Aire (Ministerio del Medio Ambiente):**
  - Portal Oficial: [https://sinca.mma.gob.cl](https://sinca.mma.gob.cl)
  - API Listado Estaciones: [https://sinca.mma.gob.cl/index.php/json/listado](https://sinca.mma.gob.cl/index.php/json/listado)
  - Calidad del Aire Regional: [https://sinca.mma.gob.cl/index.php/region/index/id/M](https://sinca.mma.gob.cl/index.php/region/index/id/M)
- **Áreas Silvestres Protegidas y Humedales Urbanos (MMA / CONAF):**
  - Catastro Nacional Áreas Protegidas: [https://areasprotegidas.mma.gob.cl](https://areasprotegidas.mma.gob.cl)
  - Registro Nacional Humedales Urbanos Ley 21.202: [https://humedaleschile.mma.gob.cl](https://humedaleschile.mma.gob.cl)
- **RETC — Registro de Emisiones y Transferencia de Contaminantes (MMA):**
  - Datos Abiertos RETC: [https://datosretc.mma.gob.cl](https://datosretc.mma.gob.cl)
- **SNIFA — Superintendencia del Medio Ambiente (SMA):**
  - Datos Abiertos SNIFA: [https://snifa.sma.gob.cl/DatosAbiertos](https://snifa.sma.gob.cl/DatosAbiertos)

---

## 🕷️ 5. Spiderweb en Google Cloud (Cloud Run & Cloud Scheduler)

- **Cloud Run Service:** `spiderweb-daily-sync` (Región `southamerica-west1`)
- **Cloud Scheduler Job:** `spiderweb-medio-ambiente-satelital`
  - **Ubicación:** `us-central1`
  - **Frecuencia:** Todos los días a las 08:00 hrs (`0 8 * * *`, America/Santiago)
  - **Endpoint:** `https://spiderweb-daily-sync-271964478709.southamerica-west1.run.app/spiderweb/medio_ambiente`
  - **Propósito:** Sincronización continua de focos satelitales NASA FIRMS de las últimas 24 horas, eventos sociopolíticos GDELT y mediciones horarias de calidad del aire SINCA MMA.

---

> actualiza el repo en github de los hitos. deja las fuentes con sus enlaces muy precisos y revisa y recontra revisa que los datos ingresados sean correctos y los enlaces funcionen. Y carga ordenadamente a google drive todos los archivos que descargaste y usaste para la ingesta de datos a google cloud. todo esto para trazabilidad. ademas actualiza el spiderweb en google cloud para google cloud run y google cloud scheduler. deja esta frase actualizada en el repo para trazabilidad
