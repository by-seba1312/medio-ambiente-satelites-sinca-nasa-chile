"""
HITO MA1: Ingesta Oficial de:
1. Focos Satelitales NASA FIRMS (MODIS, VIIRS Suomi-NPP, NOAA-20, NOAA-21) para Chile
2. Eventos Sociales y Políticos de Chile desde GDELT Project (gdelt-bq.gdeltv2.events_partitioned)
"""

import io
import os
import sys
import urllib.request
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from google.cloud import bigquery, storage

sys.stdout.reconfigure(encoding='utf-8')

PROJECT_ID = "project-0ab8074c-14aa-434e-849"
LOCATION = "southamerica-west1"
GCS_BUCKET = "chile-macro-bronze-0ab8074c"
GCS_PREFIX = "medio_ambiente_satelital"
LOCAL_RAW = r"g:\Mi unidad\Datos Publicos\08_Satelites_NASA_FIRMS_GDELT_y_Medio_Ambiente\raw_parquet"

os.makedirs(LOCAL_RAW, exist_ok=True)

bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

CAMEO_EVENT_MAP = {
    '01': 'Hacer declaración pública',
    '02': 'Apelar o solicitar acción',
    '03': 'Expresar intención de cooperar',
    '04': 'Consultar o sostener reuniones',
    '05': 'Compromiso diplomático o legislativo',
    '06': 'Cooperación material económica/técnica',
    '07': 'Proveer ayuda o asistencia',
    '08': 'Ceder o hacer concesiones',
    '09': 'Investigar judicial o administrativamente',
    '10': 'Demandas o requerimientos formales',
    '11': 'Desaprobar o criticar públicamente',
    '12': 'Rechazar propuesta o mediación',
    '13': 'Amenazar con sanciones o fuerza',
    '14': 'Protestas, huelgas o manifestaciones',
    '15': 'Demostración de fuerza policial/militar',
    '16': 'Reducir relaciones diplomáticas/comerciales',
    '17': 'Coaccionar o imponer restricciones',
    '18': 'Asalto o agresión armada',
    '19': 'Lucha armada o enfrentamiento violento',
    '20': 'Uso de violencia destructiva masiva'
}

def paso_1_gdelt():
    print("=" * 60)
    print("PASO 1: Procesando eventos de Chile desde GDELT Project...")
    
    local_file = os.path.join(LOCAL_RAW, "fact_gdelt_eventos_sociales_politicos_chile.parquet")
    
    if os.path.exists(local_file):
        print(f"Leyendo archivo parquet local existente: {local_file}")
        df_gdelt = pd.read_parquet(local_file)
    else:
        query = """
        SELECT 
          GLOBALEVENTID AS global_event_id,
          PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS fecha_evento,
          Year AS anio,
          Actor1Name AS actor1_nombre,
          Actor1Type1Code AS actor1_tipo_cameo,
          Actor2Name AS actor2_nombre,
          Actor2Type1Code AS actor2_tipo_cameo,
          EventRootCode AS event_root_code_cameo,
          QuadClass AS quad_class,
          GoldsteinScale AS goldstein_scale_impacto,
          NumMentions AS num_menciones_prensa,
          NumArticles AS num_articulos_fuente,
          AvgTone AS tono_promedio_sentimiento,
          ActionGeo_FullName AS ubicacion_nombre,
          ActionGeo_Lat AS latitud,
          ActionGeo_Long AS longitud,
          SOURCEURL AS source_url
        FROM `gdelt-bq.gdeltv2.events_partitioned`
        WHERE _PARTITIONDATE >= '2025-01-01'
          AND ActionGeo_CountryCode = 'CI'
          AND SQLDATE IS NOT NULL
          AND GLOBALEVENTID IS NOT NULL
        """
        us_bq_client = bigquery.Client(project=PROJECT_ID, location="US")
        df_gdelt = us_bq_client.query(query).to_dataframe()
        print(f"Total eventos GDELT Chile extraídos (2025-2026): {len(df_gdelt):,} filas")
        
        df_gdelt['glosa_tipo_evento'] = df_gdelt['event_root_code_cameo'].apply(
            lambda c: CAMEO_EVENT_MAP.get(str(c).zfill(2), f'Evento CAMEO {c}') if pd.notnull(c) else 'Evento no especificado'
        )
        
    df_gdelt['global_event_id'] = df_gdelt['global_event_id'].astype('int64')
    df_gdelt['anio'] = df_gdelt['anio'].astype('int64')
    df_gdelt['quad_class'] = df_gdelt['quad_class'].fillna(0).astype('int64')
    df_gdelt['num_menciones_prensa'] = df_gdelt['num_menciones_prensa'].fillna(0).astype('int64')
    df_gdelt['num_articulos_fuente'] = df_gdelt['num_articulos_fuente'].fillna(0).astype('int64')
    df_gdelt['goldstein_scale_impacto'] = df_gdelt['goldstein_scale_impacto'].astype('float64')
    df_gdelt['tono_promedio_sentimiento'] = df_gdelt['tono_promedio_sentimiento'].astype('float64')
    df_gdelt['latitud'] = df_gdelt['latitud'].astype('float64')
    df_gdelt['longitud'] = df_gdelt['longitud'].astype('float64')
    df_gdelt['ingested_at'] = pd.Timestamp.now(tz=timezone.utc)
    
    # Asegurar que no contenga columnas innecesarias
    cols_to_keep = [
        'global_event_id', 'fecha_evento', 'anio', 'actor1_nombre', 'actor1_tipo_cameo',
        'actor2_nombre', 'actor2_tipo_cameo', 'event_root_code_cameo', 'glosa_tipo_evento',
        'quad_class', 'goldstein_scale_impacto', 'num_menciones_prensa', 'num_articulos_fuente',
        'tono_promedio_sentimiento', 'ubicacion_nombre', 'latitud', 'longitud',
        'source_url', 'ingested_at'
    ]
    df_gdelt = df_gdelt[[c for c in cols_to_keep if c in df_gdelt.columns]]
    
    # Guardar en Apache Parquet ZSTD 19
    df_gdelt.to_parquet(local_file, compression='zstd', compression_level=19, index=False)
    print(f"Guardado localmente en: {local_file} ({os.path.getsize(local_file) / 1024:.1f} KB)")
    
    # Subir a GCS Bronze
    bucket = storage_client.bucket(GCS_BUCKET)
    gcs_blob = f"{GCS_PREFIX}/fact_gdelt_eventos_sociales_politicos_chile.parquet"
    blob = bucket.blob(gcs_blob)
    blob.upload_from_filename(local_file)
    print(f"Subido a GCS: gs://{GCS_BUCKET}/{gcs_blob}")
    
    # Cargar en tabla staging
    stg_table = f"{PROJECT_ID}.core_medio_ambiente_satelital.stg_gdelt_chile"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    uri = f"gs://{GCS_BUCKET}/{gcs_blob}"
    bq_client.load_table_from_uri(uri, stg_table, job_config=job_config, location=LOCATION).result()
    print("Staging GDELT cargado.")
    
    # Crear tabla final particionada y clusterizada con cruce espacial
    final_table = f"{PROJECT_ID}.core_medio_ambiente_satelital.fact_gdelt_eventos_sociales_politicos_chile"
    create_sql = f"""
    CREATE OR REPLACE TABLE `{final_table}`
    PARTITION BY fecha_evento
    CLUSTER BY quad_class, event_root_code_cameo, cut_comuna
    AS
    SELECT 
      g.global_event_id,
      g.fecha_evento,
      g.anio,
      g.actor1_nombre,
      g.actor1_tipo_cameo,
      g.actor2_nombre,
      g.actor2_tipo_cameo,
      g.event_root_code_cameo,
      g.glosa_tipo_evento,
      g.quad_class,
      g.goldstein_scale_impacto,
      g.num_menciones_prensa,
      g.num_articulos_fuente,
      g.tono_promedio_sentimiento,
      g.ubicacion_nombre,
      c.COD_REGION AS codigo_region,
      LPAD(CAST(c.CUT AS STRING), 5, '0') AS cut_comuna,
      g.latitud,
      g.longitud,
      IF(g.latitud IS NOT NULL AND g.longitud IS NOT NULL, ST_GEOGPOINT(g.longitud, g.latitud), NULL) AS geometria_punto,
      g.source_url,
      g.ingested_at
    FROM `{stg_table}` g
    LEFT JOIN `{PROJECT_ID}.ine_gold_gis.geo_comunas_censales` c
      ON g.latitud IS NOT NULL AND g.longitud IS NOT NULL
      AND ST_CONTAINS(c.geometria, ST_GEOGPOINT(g.longitud, g.latitud));
    """
    print("Creando tabla final fact_gdelt_eventos_sociales_politicos_chile...")
    bq_client.query(create_sql, location=LOCATION).result()
    
    # Limpiar staging
    bq_client.delete_table(stg_table, not_found_ok=True)
    print("Tabla final fact_gdelt_eventos_sociales_politicos_chile creada con éxito.")

def paso_2_nasa_firms():
    print("=" * 60)
    print("PASO 2: Descargando focos satelitales oficiales NASA FIRMS (MODIS y VIIRS)...")
    
    urls = {
        'VIIRS_SNPP': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_South_America_7d.csv',
        'VIIRS_NOAA20': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_South_America_7d.csv',
        'VIIRS_NOAA21': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_South_America_7d.csv',
        'MODIS_TERRA_AQUA': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_South_America_7d.csv'
    }
    
    all_focos = []
    
    for sat_name, url in urls.items():
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as resp:
                df = pd.read_csv(io.BytesIO(resp.read()))
                print(f"Descargado {sat_name}: {len(df)} registros para Sudamérica")
                
                chile = df[(df['latitude'] >= -56.5) & (df['latitude'] <= -17.5) & 
                           (df['longitude'] >= -76.0) & (df['longitude'] <= -66.0)].copy()
                
                chile['satelite_instrumento'] = sat_name
                chile['fuente_oficial_url'] = url
                
                if 'bright_ti4' in chile.columns:
                    chile['brillo_termico_kelvin'] = chile['bright_ti4'].astype(float)
                elif 'brightness' in chile.columns:
                    chile['brillo_termico_kelvin'] = chile['brightness'].astype(float)
                else:
                    chile['brillo_termico_kelvin'] = np.nan
                    
                chile['potencia_radiativa_fuego_frp_mw'] = chile['frp'].astype(float) if 'frp' in chile.columns else np.nan
                
                if 'confidence' in chile.columns:
                    chile['nivel_confianza'] = chile['confidence'].astype(str).str.upper()
                else:
                    chile['nivel_confianza'] = 'NOMINAL'
                    
                chile['dia_o_noche'] = chile['daynight'].astype(str) if 'daynight' in chile.columns else 'D'
                
                all_focos.append(chile)
                print(f"  -> {len(chile)} focos en territorio chileno")
        except Exception as e:
            print(f"Error descargando {sat_name}: {e}")
            
    df_firms = pd.concat(all_focos, ignore_index=True)
    print(f"Total focos satelitales NASA FIRMS Chile: {len(df_firms):,}")
    
    df_firms['fecha_captura'] = pd.to_datetime(df_firms['acq_date']).dt.date
    
    def make_timestamp(row):
        t_str = str(row['acq_time']).zfill(4)
        h, m = int(t_str[:2]), int(t_str[2:])
        return datetime(row['fecha_captura'].year, row['fecha_captura'].month, row['fecha_captura'].day, h, m, tzinfo=timezone.utc)
    
    df_firms['fecha_hora_utc'] = df_firms.apply(make_timestamp, axis=1)
    
    df_firms['id_foco_satelital'] = df_firms.apply(
        lambda r: f"NASA_{r['satelite_instrumento']}_{r['fecha_captura'].strftime('%Y%m%d')}_{str(r['acq_time']).zfill(4)}_{abs(r['latitude']):.4f}S_{abs(r['longitude']):.4f}W",
        axis=1
    )
    
    df_firms['latitud'] = df_firms['latitude'].astype('float64')
    df_firms['longitud'] = df_firms['longitude'].astype('float64')
    df_firms['ingested_at'] = pd.Timestamp.now(tz=timezone.utc)
    
    cols = [
        'id_foco_satelital', 'fecha_captura', 'fecha_hora_utc', 'satelite_instrumento',
        'latitud', 'longitud', 'brillo_termico_kelvin',
        'potencia_radiativa_fuego_frp_mw', 'nivel_confianza', 'dia_o_noche',
        'fuente_oficial_url', 'ingested_at'
    ]
    df_firms = df_firms[cols].drop_duplicates(subset=['id_foco_satelital'])
    
    # Guardar en Parquet ZSTD 19
    local_file = os.path.join(LOCAL_RAW, "fact_nasa_firms_focos_calor_satelital_gis.parquet")
    df_firms.to_parquet(local_file, compression='zstd', compression_level=19, index=False)
    print(f"Guardado localmente en: {local_file} ({os.path.getsize(local_file) / 1024:.1f} KB)")
    
    # Subir a GCS Bronze
    bucket = storage_client.bucket(GCS_BUCKET)
    gcs_blob = f"{GCS_PREFIX}/fact_nasa_firms_focos_calor_satelital_gis.parquet"
    blob = bucket.blob(gcs_blob)
    blob.upload_from_filename(local_file)
    print(f"Subido a GCS: gs://{GCS_BUCKET}/{gcs_blob}")
    
    # Cargar en staging
    stg_table = f"{PROJECT_ID}.core_medio_ambiente_satelital.stg_nasa_firms_chile"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    uri = f"gs://{GCS_BUCKET}/{gcs_blob}"
    bq_client.load_table_from_uri(uri, stg_table, job_config=job_config, location=LOCATION).result()
    print("Staging NASA FIRMS cargado.")
    
    # Crear tabla final particionada y clusterizada con cruce espacial y distancia
    final_table = f"{PROJECT_ID}.core_medio_ambiente_satelital.fact_nasa_firms_focos_calor_satelital_gis"
    create_sql = f"""
    CREATE OR REPLACE TABLE `{final_table}`
    PARTITION BY fecha_captura
    CLUSTER BY cut_comuna, satelite_instrumento, nivel_confianza
    AS
    WITH base_focos AS (
      SELECT 
        f.id_foco_satelital,
        f.fecha_captura,
        f.fecha_hora_utc,
        f.satelite_instrumento,
        f.latitud,
        f.longitud,
        ST_GEOGPOINT(f.longitud, f.latitud) AS geometria_punto,
        f.brillo_termico_kelvin,
        f.potencia_radiativa_fuego_frp_mw,
        f.nivel_confianza,
        f.dia_o_noche,
        c.COD_REGION AS codigo_region,
        LPAD(CAST(c.CUT AS STRING), 5, '0') AS cut_comuna,
        c.COMUNA AS nombre_comuna,
        f.fuente_oficial_url,
        f.ingested_at
      FROM `{stg_table}` f
      LEFT JOIN `{PROJECT_ID}.ine_gold_gis.geo_comunas_censales` c
        ON ST_CONTAINS(c.geometria, ST_GEOGPOINT(f.longitud, f.latitud))
    ),
    distancias AS (
      SELECT 
        b.id_foco_satelital,
        MIN(ST_DISTANCE(b.geometria_punto, m.geometria)) AS dist_m
      FROM base_focos b
      JOIN `{PROJECT_ID}.ine_gold_gis.geo_manzanas_censales_2024` m
        ON ST_DWITHIN(b.geometria_punto, m.geometria, 25000)
      GROUP BY b.id_foco_satelital
    )
    SELECT 
      b.id_foco_satelital,
      b.fecha_captura,
      b.fecha_hora_utc,
      b.satelite_instrumento,
      b.latitud,
      b.longitud,
      b.geometria_punto,
      b.brillo_termico_kelvin,
      b.potencia_radiativa_fuego_frp_mw,
      b.nivel_confianza,
      b.dia_o_noche,
      b.codigo_region,
      b.cut_comuna,
      b.nombre_comuna,
      ROUND(d.dist_m, 2) AS distancia_a_viviendas_censo_m,
      b.fuente_oficial_url,
      b.ingested_at
    FROM base_focos b
    LEFT JOIN distancias d ON b.id_foco_satelital = d.id_foco_satelital;
    """
    print("Creando tabla final fact_nasa_firms_focos_calor_satelital_gis...")
    bq_client.query(create_sql, location=LOCATION).result()
    
    # Limpiar staging
    bq_client.delete_table(stg_table, not_found_ok=True)
    print("Tabla final fact_nasa_firms_focos_calor_satelital_gis creada con éxito.")

if __name__ == "__main__":
    paso_1_gdelt()
    paso_2_nasa_firms()
    print("============================================================")
    print(">>> HITO MA1 COMPLETADO EXITOSAMENTE AL 100%. <<<")
    print("============================================================")
