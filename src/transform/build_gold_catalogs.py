import os
import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)

PATH_SILVER = os.path.expanduser("~/projects/electoral_os/data/2_silver/*.parquet")
PATH_GOLD = os.path.expanduser("~/projects/electoral_os/data/3_gold")
os.makedirs(PATH_GOLD, exist_ok=True)

def construir_capa_gold():
    con = duckdb.connect(database=':memory:')
    logging.info("Iniciando consolidación de catálogos maestros en la Capa Gold...")

    # 1. Compilar catálogo maestro de Geografía (Entity Resolution) Enforzando Unión por Nombre
    logging.info("Sintetizando inventario único de Entidades Geográficas...")
    
    query_geo = r"""
        SELECT DISTINCT
            UPPER(TRIM(regexp_replace(departamento, '\s+', ' ', 'g'))) AS departamento_raw,
            UPPER(TRIM(regexp_replace(distrito, '\s+', ' ', 'g'))) AS distrito_raw
        FROM read_parquet('{path_sources}', union_by_name=True)
        WHERE departamento IS NOT NULL AND distrito IS NOT NULL
        ORDER BY 1, 2
    """.format(path_sources=PATH_SILVER)
    
    try:
        path_dim_geo = os.path.join(PATH_GOLD, "dim_geografia_maestra.parquet")
        con.execute(f"COPY ({query_geo}) TO '{path_dim_geo}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');")
        
        total_distritos = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path_dim_geo}')").fetchone()[0]
        logging.info(f"[GOLD COMMIT] Catálogo de geografía unificado. Registros únicos: {total_distritos}")
        
    except Exception as e:
        logging.error(f"Fallo en la consolidación geográfica: {str(e)}")

    # 2. Compilar catálogo maestro de Partidos/Listas Enforzando Unión por Nombre
    logging.info("Sintetizando inventario de Actores Políticos...")
    query_partidos = r"""
        SELECT 
            UPPER(TRIM(partido)) AS partido_raw,
            COUNT(*) AS volumen_menciones
        FROM read_parquet('{path_sources}', union_by_name=True)
        WHERE partido IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
    """.format(path_sources=PATH_SILVER)
    
    try:
        path_dim_partidos = os.path.join(PATH_GOLD, "dim_partidos_maestros.parquet")
        con.execute(f"COPY ({query_partidos}) TO '{path_dim_partidos}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');")
        
        total_partidos = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path_dim_partidos}')").fetchone()[0]
        logging.info(f"[GOLD COMMIT] Catálogo de partidos unificado. Variaciones detectadas: {total_partidos}")
        
    except Exception as e:
        logging.error(f"Fallo en la consolidación de partidos: {str(e)}")

    con.close()
    logging.info("Infraestructura de la Capa Gold desplegada con éxito.")

if __name__ == "__main__":
    construir_capa_gold()
