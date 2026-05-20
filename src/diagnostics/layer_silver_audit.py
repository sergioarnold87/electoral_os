import duckdb
import os

def auditar_toda_la_capa_silver():
    silver_dir = os.path.expanduser("~/projects/electoral_os/data/2_silver/")
    
    if not os.path.exists(silver_dir):
        print(f"❌ No se encuentra la carpeta de la capa Silver en: {silver_dir}")
        return
        
    con = duckdb.connect()
    archivos_parquet = [f for f in os.listdir(silver_dir) if f.endswith(".parquet")]
    archivos_parquet.sort()
    
    print("🔬 INICIANDO AUDITORÍA FORENSE INTEGRAL DINÁMICA (100% CAPA SILVER)")
    print(f"📁 Directorio controlado: {silver_dir}")
    print(f"📦 Total de archivos Parquet detectados: {len(archivos_parquet)}")
    print("=" * 90)
    
    for idx, archivo in enumerate(archivos_parquet, 1):
        full_path = os.path.join(silver_dir, archivo)
        
        try:
            # 1. Conteo total de registros utilizando la función de tabla correctamente en el FROM
            total_filas = con.execute(f"SELECT COUNT(*) FROM read_parquet('{full_path}')").fetchone()[0]
            
            # 2. Extracción limpia de columnas usando la vista nativa de DuckDB para evitar el Binder Error
            con.execute(f"CREATE OR REPLACE TEMPORARY VIEW v_temp_audit AS SELECT * FROM read_parquet('{full_path}') LIMIT 0")
            info_columnas = con.execute("PRAGMA table_info('v_temp_audit')").fetchall()
            columnas_nombres = [c[1] for c in info_columnas]
            
            print(f"\n📄 [{idx}/{len(archivos_parquet)}] ARCHIVO: {archivo}")
            print(f"   -> Volumen: {total_filas:,} registros blindados.")
            print(f"   -> Estructura (Columnas): {', '.join(columnas_nombres)}")
            
            # 3. Controles avanzados de calidad de datos
            columnas_upper = [c.upper() for c in columnas_nombres]
            
            if "CEDULA" in columnas_upper:
                idx_col = columnas_upper.index("CEDULA")
                col_name = columnas_nombres[idx_col]
                nulos_ci = con.execute(f"SELECT COUNT(*) FROM read_parquet('{full_path}') WHERE {col_name} IS NULL").fetchone()[0]
                print(f"   -> Control Data Quality (Cédulas Nulas): {nulos_ci} [{'✅ OK' if nulos_ci == 0 else '❌ REVISAR'}]")
                
            if "VOTOS" in columnas_upper:
                idx_col = columnas_upper.index("VOTOS")
                col_name = columnas_nombres[idx_col]
                nulos_votos = con.execute(f"SELECT COUNT(*) FROM read_parquet('{full_path}') WHERE {col_name} IS NULL").fetchone()[0]
                print(f"   -> Control Data Quality (Votos Nulos): {nulos_votos} [{'✅ OK' if nulos_votos == 0 else '❌ REVISAR'}]")
                
            if "VOTOS_PREFERENCIALES" in columnas_upper:
                idx_col = columnas_upper.index("VOTOS_PREFERENCIALES")
                col_name = columnas_nombres[idx_col]
                nulos_pref = con.execute(f"SELECT COUNT(*) FROM read_parquet('{full_path}') WHERE {col_name} IS NULL").fetchone()[0]
                print(f"   -> Control Data Quality (Votos Pref Nulos): {nulos_pref} [{'✅ OK' if nulos_pref == 0 else '❌ REVISAR'}]")
                
        except Exception as e:
            print(f"\n❌ Error al interrogar el archivo {archivo}: {str(e)}")
            
    print("\n" + "=" * 90)
    print("🔬 INSPECCIÓN DINÁMICA DE INFRAESTRUCTURA FINALIZADA.")

if __name__ == "__main__":
    auditar_toda_la_capa_silver()
