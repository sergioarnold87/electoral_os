import os
import re
import pdfplumber
import pandas as pd
import gc

def procesar_bloque_2021(pdf_path, inicio, fin, re_nominal, deptos_validos):
    records_bloque = []
    current_depto = "NACIONAL"
    current_distrito = "NO ESPECIFICADO"
    current_cargo = "NO ESPECIFICADO"
    
    with pdfplumber.open(pdf_path) as pdf:
        total_real = len(pdf.pages)
        rango_fin = min(fin, total_real)
        
        for idx in range(inicio, rango_fin):
            page = pdf.pages[idx]
            texto = page.extract_text()
            if not texto:
                continue
                
            lineas = [l.strip() for l in texto.split("\n") if l.strip()]
            
            # Escaneo de contexto de cabecera en las primeras 12 líneas
            for l in lineas[:12]:
                l_upper = l.upper()
                
                # Captura de Departamento tradicional
                for d in deptos_validos:
                    if d in l_upper:
                        current_depto = d
                        break
                
                # Captura dinámica de Distrito (formato horizontal y vertical)
                if "DISTRITO:" in l_upper:
                    match_dist = re.search(r"Distrito:\s*\d*\s*(.+)$", l, re.IGNORECASE)
                    if match_dist:
                        current_distrito = match_dist.group(1).strip().upper()
                elif "SAN IGNACIO" in l_upper:
                    current_distrito = "SAN IGNACIO"
                elif "SAN JUAN BAUTISTA" in l_upper:
                    current_distrito = "SAN JUAN BAUTISTA"
                
                # Captura estricta del Cargo Municipal
                if "INTENDENTE" in l_upper and "GUARISMOS" not in l_upper:
                    current_cargo = "INTENDENTE"
                elif "CONCEJAL TITULAR" in l_upper or "CONCEJALES TITULARES" in l_upper:
                    current_cargo = "CONCEJAL TITULAR"
                elif "CONCEJAL SUPLENTE" in l_upper or "CONCEJALES SUPLENTES" in l_upper:
                    current_cargo = "CONCEJAL SUPLENTE"

            # Extracción de registros nominales
            for linea in lineas:
                match = re_nominal.match(linea)
                if match:
                    try:
                        orden = int(match.group(1))
                        lista = int(match.group(2))
                        movimiento = match.group(3).strip().upper()
                        cedula_str = match.group(4).replace(".", "")
                        candidato = match.group(5).strip().upper()
                        votos = int(match.group(6))
                        
                        records_bloque.append({
                            "ano": 2021,
                            "departamento": current_depto,
                            "distrito": current_distrito,
                            "cargo": current_cargo,
                            "orden": orden,
                            "lista": lista,
                            "movimiento": movimiento,
                            "cedula": int(cedula_str),
                            "candidato": candidato,
                            "votos_preferenciales": votos
                        })
                    except Exception:
                        continue
            
            page.flush_cache()
            
    return records_bloque

def parse_anr_2021():
    pdf_path = os.path.expanduser("~/projects/electoral_os/data/1_bronze/anr/INFORME_FINAL_INTERNAS_PARTIDARIAS_2021_compressed.pdf")
    output_path = os.path.expanduser("~/projects/electoral_os/data/2_silver/silver_anr_proclamados_2021.parquet")
    
    if not os.path.exists(pdf_path):
        print(f"❌ No se encuentra el origen: {pdf_path}")
        return

    print("🚀 Iniciando extracción del Nacional Comprimido 2021 por Bloques...")
    
    re_nominal = re.compile(r"^(\d+)\s+(\d+)\s+(.+?)\s+([\d.]+)\s+(.+?)\s+(\d+)$")
    all_records = []
    
    deptos_validos = [
        "CONCEPCION", "SAN PEDRO", "CORDILLERA", "GUAIRA", "CAAGUAZU", "CAAZAPA",
        "ITAPUA", "MISIONES", "PARAGUARI", "ALTO PARANA", "CENTRAL", "NEEMBUCU",
        "AMAMBAY", "CANINDEYU", "PRESIDENTE HAYES", "BOQUERON", "ALTO PARAGUAY",
        "CAPITAL"
    ]

    with pdfplumber.open(pdf_path) as pdf:
        total_paginas = len(pdf.pages)
    
    tamanio_bloque = 500
    
    for inicio in range(0, total_paginas, tamanio_bloque):
        fin = inicio + tamanio_bloque
        print(f"⏳ Procesando bloque: páginas {inicio + 1} a {min(fin, total_paginas)} de {total_paginas}...")
        records_lote = procesar_bloque_2021(pdf_path, inicio, fin, re_nominal, deptos_validos)
        all_records.extend(records_lote)
        del records_lote
        gc.collect()

    if not all_records:
        print("⚠️ Alerta: No se extrajeron registros nominales del archivo nacional.")
        return

    print("💾 Escribiendo la base consolidada 2021 en Silver Parquet...")
    df = pd.DataFrame(all_records)
    
    # Limpieza final de strings remanentes en los distritos clave
    df['distrito'] = df['distrito'].apply(lambda x: "SAN IGNACIO" if "SAN IGNACIO" in str(x) else x)
    df['distrito'] = df['distrito'].apply(lambda x: "SAN JUAN BAUTISTA" if "SAN JUAN BAUTISTA" in str(x) else x)
    
    df = df.drop_duplicates()
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"✅ Proceso terminado. {len(df)} registros nacionales indexados.")

if __name__ == "__main__":
    parse_anr_2021()
