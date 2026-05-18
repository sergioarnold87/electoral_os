import duckdb
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

PATH_BRONZE = os.path.expanduser("~/projects/electoral_os/data/1_bronze/tsje/bronze_tsje_resultados_*.parquet")

def ejecutar_diagnostico():
    logging.info("Iniciando EDA tolerante a falhas de esquema (Schema-Drift Aware)...")
    
    con = duckdb.connect(database=':memory:')
    
    # 1. Auditoria de Cobertura Física e Volumetria com tolerância a discrepância de colunas
    logging.info("1. EVALUACIÓN DE VOLUMETRÍA POR ASSET HISTÓRICO:")
    query_volume = f"""
        SELECT 
            regexp_extract(file_path, '([0-9]{{4}}_[0-9]{{4}}|[0-9]{{4}})', 1) AS periodo_split,
            COUNT(*) AS total_registros
        FROM read_parquet('{PATH_BRONZE}', filename=true, union_by_name=True) AS t(file_path)
        GROUP BY 1
        ORDER BY 1 ASC;
    """
    try:
        print("\n" + "="*60)
        con.sql(query_volume).show()
        print("="*60)
    except Exception as e:
        logging.error(f"Falha na leitura do volume: {str(e)}")
        return

    # 2. Inspeção do catálogo unificado de colunas generadas pela fusão
    logging.info("2. COMPILACIÓN DE COLUMNAS DISPONIBLES EN EL ALINEAMIENTO:")
    query_schema = f"DESCRIBE SELECT * FROM read_parquet('{PATH_BRONZE}', union_by_name=True);"
    try:
        print("\n" + "="*60)
        con.sql(query_schema).show()
        print("="*60)
    except Exception as e:
        logging.error(f"Falha na leitura do esquema: {str(e)}")

    # 3. Análise de Densidade de Identificadores Limpos
    logging.info("3. CONTROL DE COMPLETITUD EN ATRIBUTOS COMUNES:")
    query_nulls = f"""
        SELECT 
            COUNT(*) AS universo_total_filas,
            COUNT(departamento) AS validos_departamento,
            (100.0 * (COUNT(*) - COUNT(departamento)) / COUNT(*))::NUMERIC(5,2) AS pct_nulos_depto,
            COUNT(distrito) AS validos_distrito,
            COUNT(local) AS validos_local,
            COUNT(mesa) AS validos_mesa
        FROM read_parquet('{PATH_BRONZE}', union_by_name=True);
    """
    try:
        print("\n" + "="*60)
        con.sql(query_nulls).show()
        print("="*60)
    except Exception as e:
        logging.warning(f"Campos comuns não alinhados nos arquivos Parquet: {str(e)}")

    con.close()
    logging.info("Diagnóstico estrutural finalizado.")

if __name__ == "__main__":
    ejecutar_diagnostico()
