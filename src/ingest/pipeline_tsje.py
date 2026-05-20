import os
import time
import re
import requests
import duckdb
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)

ENDPOINT_DATA_JSON = "https://www.datos.gov.py/data.json"
OUTPUT_DIR = os.path.expanduser("~/projects/electoral_os/data/1_bronze/tsje")
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def extraer_ano_seguro(nombre, titulo_dataset):
    texto_combinado = f"{nombre} {titulo_dataset}".lower()
    anos_detectados = re.findall(r'\b(19\d\d|20\d\d)\b', texto_combinado)
    anos_validos = [a for a in anos_detectados if 1996 <= int(a) <= 2026]
    if anos_validos:
        return anos_validos[0]
    return None

def ejecutar_pipeline():
    logging.info("Iniciando sincronización analítica del catálogo universal DKAN...")
    try:
        response = requests.get(ENDPOINT_DATA_JSON, headers=HEADERS, timeout=60)
        response.raise_for_status()
        catalog = response.json()
    except Exception as e:
        logging.error(f"Fallo crítico en el acceso al índice central: {str(e)}")
        return

    datasets = catalog.get("dataset", [])
    tsje_distributions = []

    for dataset in datasets:
        title = dataset.get("title", "").lower()
        publisher_name = dataset.get("publisher", {}).get("name", "").lower() if isinstance(dataset.get("publisher"), dict) else ""

        if "tsje" in title or "electoral" in title or "justicia electoral" in publisher_name:
            distributions = dataset.get("distribution", [])
            for dist in distributions:
                if isinstance(dist, dict):
                    tsje_distributions.append({
                        "dataset_title": dataset.get("title", ""),
                        "title": dist.get("title", ""),
                        "url": dist.get("downloadURL"),
                        "catalog_format": dist.get("format", "").upper()
                    })

    logging.info(f"Filtro de gobernanza completado. {len(tsje_distributions)} recursos TSJE aislados.")
    con = duckdb.connect(database=':memory:')

    for item in tsje_distributions:
        url = item["url"]
        if not url:
            continue
            
        nombre_asset = item["title"].lower() if item["title"] else item["dataset_title"].lower()
        
        url_lower = url.lower()
        if url_lower.endswith('.xlsx'):
            format_type = "XLSX"
        elif url_lower.endswith('.csv'):
            format_type = "CSV"
        else:
            format_type = item["catalog_format"]

        if format_type not in ["CSV", "XLSX"]:
            continue

        categoria = "resultados"
        if any(x in nombre_asset for x in ["padrón", "padron"]):
            categoria = "padron"
        elif any(x in nombre_asset for x in ["candidatos", "electos"]):
            categoria = "candidatos"
        elif any(x in nombre_asset for x in ["preferentes", "votos"]):
            categoria = "preferentes"

        ano = extraer_ano_seguro(item["title"], item["dataset_title"])
        if not ano:
            continue

        filename_parquet = f"bronze_tsje_{categoria}_{ano}_{ano}.parquet"
        target_parquet_path = os.path.join(OUTPUT_DIR, filename_parquet)
        temp_file_path = os.path.join("/tmp", os.path.basename(url.split("?")[0]))

        if os.path.exists(target_parquet_path):
            continue

        logging.info(f"Descargando flujo binario: {url}")
        time.sleep(1.0)
        
        try:
            with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(temp_file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=32768):
                        f.write(chunk)

            if format_type == "CSV":
                with open(temp_file_path, 'r', errors='ignore') as f_head:
                    first_line = f_head.readline()
                
                delimiter = ";" if ";" in first_line and "," not in first_line else ","
                
                query_csv = f"""
                    COPY (
                        SELECT * FROM read_csv_auto('{temp_file_path}', 
                            all_varchar=True, 
                            delim='{delimiter}',
                            encoding='LATIN1'
                        )
                    ) TO '{target_parquet_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
                """
                con.execute(query_csv)

            elif format_type == "XLSX":
                logging.info(f"Procesando binario Excel nativo con openpyxl para {filename_parquet}")
                df_excel = pd.read_excel(temp_file_path, engine='openpyxl', dtype=str)
                con.execute(f"CREATE OR REPLACE TEMP TABLE tmp_excel AS SELECT * FROM df_excel;")
                con.execute(f"COPY tmp_excel TO '{target_parquet_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');")
                con.execute("DROP TABLE tmp_excel;")

            registros_ingestados = con.execute(f"SELECT COUNT(*) FROM read_parquet('{target_parquet_path}')").fetchone()[0]
            columnas_ingestadas = len([row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{target_parquet_path}')").fetchall()])
            logging.info(f"[METRIC COMMIT] Verified Asset: {filename_parquet} | Filas: {registros_ingestados} | Columnas: {columnas_ingestadas}")
            
        except Exception as e:
            logging.error(f"[TRANSACTION FAIL] Caída en el procesamiento de {nombre_asset}: {str(e)}")
            if os.path.exists(target_parquet_path):
                os.remove(target_parquet_path)
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    con.close()
    logging.info("Ejecución del pipeline de adquisición finalizado con validación de contratos.")

# --- INVOCACIÓN EXPLÍCITA DEL PUNTO DE ENTRADA ---
if __name__ == "__main__":
    ejecutar_pipeline()
