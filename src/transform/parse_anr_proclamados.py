import os
import re
import pdfplumber
import pandas as pd
import gc

def procesar_bloque_paginas(pdf_path, inicio, fin, re_votos, deptos_validos):
    records_bloque = []
    
    # Variables de estado que persisten entre páginas del mismo bloque
    current_depto = "NACIONAL"
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
            
            # Escaneo de metadatos en toda la página si es necesario, buscando contexto
            for l in lineas[:12]:
                l_upper = l.upper()
                # Si encontramos un departamento válido en cualquier parte de la cabecera, actualizamos
                for d in deptos_validos:
                    if d in l_upper:
                        current_depto = d
                        break
                # Si la línea define un cargo, actualizamos el estado
                if any(x in l.upper() for x in ["TITULAR", "SUPLENTE", "GOBERNADOR", "PRESIDENTE", "INTENDENTE", "CONCEJAL"]):
                    # Evitamos que las líneas de registros limpien el cargo
                    if not re_votos.match(l):
                        current_cargo = l.replace("*", "").strip()

            for linea in lineas:
                match = re_votos.match(linea)
                if match:
                    try:
                        orden = int(match.group(1))
                        lista = int(match.group(2))
                        movimiento = match.group(3).strip().upper()
                        cedula_str = match.group(4).replace(".", "")
                        candidato = match.group(5).strip().upper()
                        
                        # Control forense del asterisco: si está al final de la línea o del nombre del candidato
                        electo = 1 if "*" in linea or "*" in candidato else 0
                        
                        # Limpiamos el asterisco residual del nombre si existiera
                        candidato = candidato.replace("*", "").strip()
                        
                        records_bloque.append({
                            "pagina": idx + 1,
                            "departamento": current_depto,
                            "cargo": current_cargo,
                            "orden": orden,
                            "lista": lista,
                            "movimiento": movimiento,
                            "cedula": int(cedula_str),
                            "candidato": candidato,
                            "electo": electo
                        })
                    except Exception:
                        continue
            
            page.flush_cache()
            
    return records_bloque

def parse_anr_2022():
    pdf_path = os.path.expanduser("~/projects/electoral_os/data/1_bronze/anr/ANR_informe_final_Elecciones_18.dic.2022.pdf")
    output_path = os.path.expanduser("~/projects/electoral_os/data/2_silver/silver_anr_proclamados_2022.parquet")
    
    if not os.path.exists(pdf_path):
        print(f"❌ No se encuentra el origen: {pdf_path}")
        return

    print("🚀 Ejecutando Reparación de Data Quality por Segmentos...")
    
    re_votos = re.compile(r"^(\d+)\s+(\d+)\s+(.+?)\s+([\d.]+)\s+(.+?)(?:\s+\*)?$")
    all_records = []
    
    deptos_validos = [
        "CONCEPCION", "SAN PEDRO", "CORDILLERA", "GUAIRA", "CAAGUAZU", "CAAZAPA",
        "ITAPUA", "MISIONES", "PARAGUARI", "ALTO PARANA", "CENTRAL", "NEEMBUCU",
        "AMAMBAY", "CANINDEYU", "PRESIDENTE HAYES", "BOQUERON", "ALTO PARAGUAY",
        "CAPITAL", "EXTRANJERO"
    ]

    with pdfplumber.open(pdf_path) as pdf:
        total_paginas = len(pdf.pages)
    
    tamanio_bloque = 500
    
    for inicio in range(0, total_paginas, tamanio_bloque):
        fin = inicio + tamanio_bloque
        print(f"⏳ Procesando lote seguro: páginas {inicio + 1} a {min(fin, total_paginas)}...")
        records_lote = procesar_bloque_paginas(pdf_path, inicio, fin, re_votos, deptos_validos)
        all_records.extend(records_lote)
        del records_lote
        gc.collect()

    if not all_records:
        print("⚠️ Error: La matriz volvió a quedar vacía.")
        return

    print("💾 Reescribiendo Silver Parquet con datos corregidos...")
    df = pd.DataFrame(all_records)
    df = df.drop_duplicates()
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"✅ Sanación completa. {len(df)} registros relacionales guardados sin pérdidas.")

if __name__ == "__main__":
    parse_anr_2022()
