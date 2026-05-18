import duckdb
import glob
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

DIR_BRONZE = os.path.expanduser("~/projects/electoral_os/data/1_bronze/tsje/*.parquet")

def auditar_capa_bronze():
    logging.info("Iniciando escaneo atómico de archivos Parquet...")
    archivos = glob.glob(DIR_BRONZE)
    
    if not archivos:
        logging.error("No se encontraron archivos Parquet en la ruta especificada.")
        return

    con = duckdb.connect(database=':memory:')
    
    print("\n" + "="*80)
    print(f"{'ARCHIVO PARQUET':<50} | {'FILAS':<10} | {'COLUMNAS':<8}")
    print("="*80)
    
    archivos_anchos = []
    archivos_largos = []
    archivos_rotos = []

    for f in sorted(archivos):
        nombre_base = os.path.basename(f)
        try:
            # Contar registros del archivo individual
            total_filas = con.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
            
            # Extraer los nombres de las columnas
            columnas = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()]
            total_cols = len(columnas)
            
            print(f"{nombre_base:<50} | {total_filas:<10} | {total_cols:<8}")
            
            # Clasificación analítica de layouts
            if any(";" in col for col in columnas):
                archivos_rotos.append((nombre_base, "Delimitador roto (;)"))
            elif "LISTA" in columnas or "VOTOS" in columnas:
                archivos_largos.append(nombre_base)
            else:
                archivos_anchos.append(nombre_base)
                
        except Exception as e:
            print(f"{nombre_base:<50} | ERROR      | N/A")
            logging.error(f"No se pudo leer el archivo {nombre_base}: {str(e)}")

    print("="*80)
    print(f"\n[Clasificación Operativa]")
    print(f"-> Matrices Anchas (Wide Format): {len(archivos_anchos)} archivos.")
    print(f"-> Modelos Largos (Long Format):  {len(archivos_largos)} archivos.")
    print(f"-> Assets con Errores (;):       {len(archivos_rotos)} archivos.")
    
    if archivos_rotos:
        print("\n[ALERT] Assets que requieren corrección de delimitador:")
        for r in archivos_rotos:
            print(f"  ! {r[0]} -> Motivo: {r[1]}")

    con.close()

if __name__ == "__main__":
    auditar_capa_bronze()
