import os
import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)

PATH_BRONZE = os.path.expanduser("~/projects/electoral_os/data/1_bronze/tsje")
PATH_SILVER = os.path.expanduser("~/projects/electoral_os/data/2_silver")
os.makedirs(PATH_SILVER, exist_ok=True)

def procesar_capa_silver():
    con = duckdb.connect(database=':memory:')
    
    if not os.path.exists(PATH_BRONZE):
        logging.error("Directorio Bronze ausente. Abortando transformación.")
        return
        
    archivos = [f for f in os.listdir(PATH_BRONZE) if f.endswith('.parquet')]
    logging.info(f"Iniciando normalización Silver. Detectados {len(archivos)} activos intermedios.")

    # Mitigación perimetral: Incluimos variaciones de eñes en la exclusión estática
    columnas_estaticas_lower = ['departamento', 'distrito', 'local', 'nromesa', 'ano', 'año', 'categoria']

    for archivo in archivos:
        path_completo_bronze = os.path.join(PATH_BRONZE, archivo)
        path_completo_silver = os.path.join(PATH_SILVER, f"silver_{archivo}")
        
        # --- CONTROL DE IDEMPOTENCIA (CHECKPOINT) ---
        if os.path.exists(path_completo_silver):
            logging.info(f"[SKIP 304] Asset ya normalizado en Silver: silver_{archivo}. Saltando transacción.")
            continue
            
        logging.info(f"Analizando deriva de esquema para asset: {archivo}")
        
        try:
            # Inspección de metadatos preservando el casing tipográfico original del Parquet
            schema_info = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_completo_bronze}');").fetchall()
            
            # Filtramos las columnas de partidos manteniendo su formato exacto de origen
            columnas_partidos = [row[0] for row in schema_info if row[0].lower() not in columnas_estaticas_lower]
            
            if not columnas_partidos:
                logging.warning(f"[STAGE SKIP] El archivo {archivo} no contiene columnas dinámicas para ejecutar unpivoting.")
                continue

            # Construcción de una proyección dinámica para normalizar 'Año' con eñe a 'ano' estándar
            select_fields = []
            for row in schema_info:
                col_original = row[0]
                if col_original.lower() == 'año':
                    select_fields.append(f'"{col_original}" AS ano')
                else:
                    select_fields.append(f'"{col_original}"')
            
            select_clause = ", ".join(select_fields)
            columnas_on_clause = ", ".join([f'"{c}"' for c in columnas_partidos])
            
            logging.info(f"Ejecutando transposición (Unpivoting) de {len(columnas_partidos)} columnas de partidos políticos...")
            
            # Query vectorial de transposición sobre la proyección normalizada
            query_unpivot = f"""
                COPY (
                    UNPIVOT (
                        SELECT {select_clause} FROM read_parquet('{path_completo_bronze}')
                    )
                    ON {columnas_on_clause}
                    INTO
                        NAME partido
                        VALUE votos
                ) TO '{path_completo_silver}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
            """
            con.execute(query_unpivot)
            
            filas_silver = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path_completo_silver}')").fetchone()[0]
            logging.info(f"[SILVER COMMIT] Estandarización exitosa: {archivo} -> Filas resultantes: {filas_silver}")
            
        except Exception as e:
            logging.error(f"[TRANSFORMATION FAIL] Error crítico procesando {archivo}: {str(e)}")
            if os.path.exists(path_completo_silver):
                os.remove(path_completo_silver)

    con.close()
    logging.info("Consolidación de la capa Silver finalizada con total conformidad contractual.")

if __name__ == "__main__":
    procesar_capa_silver()
