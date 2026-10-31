-- =============================================================================
-- PROYECTO 8: JOYA OCULTA #10 (NASA FIRMS + GDELT CHILE) + MEDIO AMBIENTE MMA/SMA
-- Dataset Silver: project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital
-- Vistas Gold:    project-0ab8074c-14aa-434e-849.gold_nacional
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital`
OPTIONS (
  location = 'southamerica-west1',
  storage_billing_model = 'PHYSICAL',
  description = 'Data Lakehouse Silver: Monitoreo Satelital NASA FIRMS (MODIS/VIIRS), Eventos Globales GDELT Chile y Ministerio del Medio Ambiente (SINCA, RETC, SMA SNIFA, SBAP)'
);

-- 1. Focos Satelitales NASA FIRMS (MODIS y VIIRS) en Chile
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_nasa_firms_focos_calor_satelital_gis` (
  id_foco_satelital STRING NOT NULL,
  fecha_captura DATE NOT NULL,
  fecha_hora_utc TIMESTAMP NOT NULL,
  satelite_instrumento STRING,
  latitud FLOAT64 NOT NULL,
  longitud FLOAT64 NOT NULL,
  geometria_punto GEOGRAPHY,
  brillo_termico_kelvin FLOAT64,
  potencia_radiativa_fuego_frp_mw FLOAT64,
  nivel_confianza STRING,
  dia_o_noche STRING,
  codigo_region INT64,
  cut_comuna STRING,
  nombre_comuna STRING,
  distancia_a_viviendas_censo_m FLOAT64,
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY fecha_captura
CLUSTER BY cut_comuna, satelite_instrumento, nivel_confianza;

-- 2. Eventos Sociopolíticos, Acuerdos y Protestas en Chile (GDELT Project)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_gdelt_eventos_sociales_politicos_chile` (
  global_event_id INT64 NOT NULL,
  fecha_evento DATE NOT NULL,
  anio INT64 NOT NULL,
  actor1_nombre STRING,
  actor1_tipo_cameo STRING,
  actor2_nombre STRING,
  actor2_tipo_cameo STRING,
  event_root_code_cameo STRING,
  glosa_tipo_evento STRING,
  quad_class INT64,
  goldstein_scale_impacto FLOAT64,
  num_menciones_prensa INT64,
  num_articulos_fuente INT64,
  tono_promedio_sentimiento FLOAT64,
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

-- 3. Dimensión Georreferenciada de Estaciones SINCA, Humedales Urbanos y Áreas Protegidas SNASPE
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.dim_estaciones_sinca_y_areas_protegidas_gis` (
  id_elemento_ambiental STRING NOT NULL,
  tipo_elemento STRING NOT NULL,
  nombre_elemento STRING,
  operador_o_administrador STRING,
  zona_saturada_o_latente_pda STRING,
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

-- 4. Hechos de Calidad del Aire SINCA (MP2.5, MP10, O3, SO2, NO2, CO) y Episodios Críticos
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_sinca_calidad_aire_y_episodios_criticos` (
  fecha_medicion DATE NOT NULL,
  anio INT64 NOT NULL,
  id_estacion_sinca STRING NOT NULL,
  cut_comuna STRING NOT NULL,
  parametro_cod STRING NOT NULL,
  concentracion_promedio_24h_ug_m3 FLOAT64,
  concentracion_max_horaria_ug_m3 FLOAT64,
  indice_calidad_aire_icap INT64,
  nivel_episodio_critico STRING,
  supera_norma_primaria_bool BOOL,
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY fecha_medicion
CLUSTER BY cut_comuna, parametro_cod, id_estacion_sinca;

-- 5. Hechos del Registro de Emisiones y Transferencia de Contaminantes (RETC)
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_mma_retc_emisiones_y_residuos_comunal` (
  anio_reporte INT64 NOT NULL,
  id_establecimiento_vu_retc INT64,
  rut_titular_num INT64,
  nombre_establecimiento STRING,
  rubro_ciiu4 STRING,
  codigo_region INT64,
  cut_comuna STRING NOT NULL,
  tipo_flujo_retc STRING NOT NULL,
  contaminante_o_residuo STRING,
  cantidad_ton_anual FLOAT64,
  destino_tratamiento STRING,
  fuente_oficial_url STRING,
  ingested_at TIMESTAMP
)
PARTITION BY RANGE_BUCKET(anio_reporte, GENERATE_ARRAY(2014, 2030, 1))
CLUSTER BY tipo_flujo_retc, cut_comuna;

-- 6. Hechos de Fiscalizaciones, Denuncias y Sanciones SNIFA-SMA
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_sma_snifa_fiscalizaciones_sanciones_y_denuncias` (
  anio INT64 NOT NULL,
  id_expediente_snifa STRING NOT NULL,
  tipo_proceso_sma STRING NOT NULL,
  unidad_fiscalizable_nombre STRING,
  categoria_economica STRING,
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

-- =============================================================================
-- VISTAS GOLD MULTIDOMINIO EN gold_nacional
-- =============================================================================

-- Vista Gold 1: Monitoreo Satelital NASA FIRMS + Calidad del Aire SINCA
CREATE OR REPLACE VIEW `project-0ab8074c-14aa-434e-849.gold_nacional.vw_monitoreo_satelital_incendios_nasa_firms_y_calidad_aire_sinca`
AS
WITH focos_agrupados AS (
  SELECT 
    fecha_captura AS fecha,
    cut_comuna,
    COUNT(1) AS total_focos_calor_nasa,
    ROUND(MAX(potencia_radiativa_fuego_frp_mw), 2) AS frp_maximo_mw,
    ROUND(AVG(potencia_radiativa_fuego_frp_mw), 2) AS frp_promedio_mw,
    ROUND(MIN(distancia_a_viviendas_censo_m), 1) AS distancia_minima_viviendas_m,
    STRING_AGG(DISTINCT satelite_instrumento, ', ' ORDER BY satelite_instrumento) AS satelites_detectores
  FROM `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_nasa_firms_focos_calor_satelital_gis`
  WHERE cut_comuna IS NOT NULL
  GROUP BY fecha_captura, cut_comuna
),
calidad_aire_agrupada AS (
  SELECT 
    fecha_medicion AS fecha,
    cut_comuna,
    ROUND(AVG(CASE WHEN parametro_cod = 'MP25' THEN concentracion_promedio_24h_ug_m3 END), 1) AS mp25_promedio_ug_m3,
    ROUND(AVG(CASE WHEN parametro_cod = 'MP10' THEN concentracion_promedio_24h_ug_m3 END), 1) AS mp10_promedio_ug_m3,
    ROUND(AVG(CASE WHEN parametro_cod = 'SO2' THEN concentracion_promedio_24h_ug_m3 END), 1) AS so2_promedio_ug_m3,
    ROUND(AVG(CASE WHEN parametro_cod = 'O3' THEN concentracion_promedio_24h_ug_m3 END), 1) AS o3_promedio_ug_m3,
    MAX(CASE 
      WHEN nivel_episodio_critico = 'EMERGENCIA_AMBIENTAL' THEN 5
      WHEN nivel_episodio_critico = 'PREEMERGENCIA' THEN 4
      WHEN nivel_episodio_critico = 'ALERTA_AMBIENTAL' THEN 3
      WHEN nivel_episodio_critico = 'REGULAR' THEN 2
      ELSE 1
    END) AS severidad_episodio_num,
    LOGICAL_OR(supera_norma_primaria_bool) AS supera_norma_aire_bool
  FROM `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_sinca_calidad_aire_y_episodios_criticos`
  GROUP BY fecha_medicion, cut_comuna
),
areas_resumen AS (
  SELECT 
    cut_comuna,
    COUNT(1) AS total_areas_protegidas_humedales,
    STRING_AGG(nombre_elemento, '; ' LIMIT 3) AS nombres_areas_protegidas,
    MAX(zona_saturada_o_latente_pda) AS pda_comunal
  FROM `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.dim_estaciones_sinca_y_areas_protegidas_gis`
  WHERE tipo_elemento != 'ESTACION_SINCA_AIRE'
  GROUP BY cut_comuna
)
SELECT 
  COALESCE(f.fecha, a.fecha) AS fecha,
  c.COD_REGION AS codigo_region,
  c.REGION AS nombre_region,
  LPAD(CAST(c.CUT AS STRING), 5, '0') AS cut_comuna,
  c.COMUNA AS nombre_comuna,
  COALESCE(f.total_focos_calor_nasa, 0) AS total_focos_calor_nasa,
  f.frp_maximo_mw,
  f.frp_promedio_mw,
  f.distancia_minima_viviendas_m,
  f.satelites_detectores,
  a.mp25_promedio_ug_m3,
  a.mp10_promedio_ug_m3,
  a.so2_promedio_ug_m3,
  a.o3_promedio_ug_m3,
  CASE a.severidad_episodio_num
    WHEN 5 THEN 'EMERGENCIA_AMBIENTAL'
    WHEN 4 THEN 'PREEMERGENCIA'
    WHEN 3 THEN 'ALERTA_AMBIENTAL'
    WHEN 2 THEN 'REGULAR'
    WHEN 1 THEN 'BUENO'
    ELSE 'SIN_MEDICION'
  END AS episodio_calidad_aire,
  COALESCE(a.supera_norma_aire_bool, FALSE) AS supera_norma_aire_bool,
  COALESCE(ar.pda_comunal, 'Sin PDA') AS pda_comunal,
  COALESCE(ar.total_areas_protegidas_humedales, 0) AS total_areas_protegidas_humedales,
  ar.nombres_areas_protegidas,
  CURRENT_TIMESTAMP() AS vista_generada_at
FROM focos_agrupados f
FULL OUTER JOIN calidad_aire_agrupada a
  ON f.fecha = a.fecha AND f.cut_comuna = a.cut_comuna
JOIN `project-0ab8074c-14aa-434e-849.ine_gold_gis.geo_comunas_censales` c
  ON COALESCE(f.cut_comuna, a.cut_comuna) = LPAD(CAST(c.CUT AS STRING), 5, '0')
LEFT JOIN areas_resumen ar
  ON COALESCE(f.cut_comuna, a.cut_comuna) = ar.cut_comuna;

-- Vista Gold 2: Pulso Social, Político y Conflictos GDELT en Chile
CREATE OR REPLACE VIEW `project-0ab8074c-14aa-434e-849.gold_nacional.vw_pulso_eventos_sociales_politicos_gdelt_y_noticias_chile`
AS
WITH gdelt_diario AS (
  SELECT 
    fecha_evento AS fecha,
    codigo_region,
    cut_comuna,
    COUNT(1) AS total_eventos_gdelt,
    COUNTIF(quad_class = 1) AS eventos_cooperacion_verbal,
    COUNTIF(quad_class = 2) AS eventos_cooperacion_material,
    COUNTIF(quad_class = 3) AS eventos_conflicto_verbal,
    COUNTIF(quad_class = 4) AS eventos_conflicto_material,
    COUNTIF(event_root_code_cameo = '14') AS eventos_protestas_manifestaciones,
    COUNTIF(event_root_code_cameo IN ('18', '19', '20')) AS eventos_fuerza_violencia,
    ROUND(AVG(goldstein_scale_impacto), 2) AS impacto_goldstein_promedio,
    ROUND(AVG(tono_promedio_sentimiento), 2) AS tono_sentimiento_promedio,
    SUM(num_menciones_prensa) AS total_menciones_prensa,
    SUM(num_articulos_fuente) AS total_articulos_prensa,
    STRING_AGG(DISTINCT glosa_tipo_evento, '; ' LIMIT 3) AS principales_tipos_eventos,
    STRING_AGG(DISTINCT actor1_nombre, ', ' LIMIT 3) AS actores_principales
  FROM `project-0ab8074c-14aa-434e-849.core_medio_ambiente_satelital.fact_gdelt_eventos_sociales_politicos_chile`
  GROUP BY fecha_evento, codigo_region, cut_comuna
)
SELECT 
  g.fecha,
  COALESCE(g.codigo_region, c.COD_REGION) AS codigo_region,
  COALESCE(c.REGION, 'Sin Región Específica') AS nombre_region,
  g.cut_comuna,
  COALESCE(c.COMUNA, 'Nivel Nacional / No Geocodificada') AS nombre_comuna,
  g.total_eventos_gdelt,
  g.eventos_cooperacion_verbal,
  g.eventos_cooperacion_material,
  g.eventos_conflicto_verbal,
  g.eventos_conflicto_material,
  g.eventos_protestas_manifestaciones,
  g.eventos_fuerza_violencia,
  g.impacto_goldstein_promedio,
  g.tono_sentimiento_promedio,
  CASE 
    WHEN g.tono_sentimiento_promedio < -3.0 THEN 'MUY_NEGATIVO_TENSO'
    WHEN g.tono_sentimiento_promedio < 0.0 THEN 'MODERADAMENTE_NEGATIVO'
    WHEN g.tono_sentimiento_promedio = 0.0 THEN 'NEUTRO'
    ELSE 'POSITIVO_FAVORABLE'
  END AS clima_social_sentimiento,
  g.total_menciones_prensa,
  g.total_articulos_prensa,
  g.principales_tipos_eventos,
  g.actores_principales,
  CURRENT_TIMESTAMP() AS vista_generada_at
FROM gdelt_diario g
LEFT JOIN `project-0ab8074c-14aa-434e-849.ine_gold_gis.geo_comunas_censales` c
  ON g.cut_comuna = LPAD(CAST(c.CUT AS STRING), 5, '0');
