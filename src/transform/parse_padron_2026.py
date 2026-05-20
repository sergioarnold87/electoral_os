import os
import re
import pdfplumber
import pandas as pd
import gc

def parse_padron_2026():
    pdf_path = os.path.expanduser("~/projects/electoral_os/data/1_bronze/anr/8---243-SAN IGNACIO.pdf")
    output_path = os.path.expanduser("~/projects/electoral_os/data/2_silver/silver_padron_san_ignacio_2026.parquet")
    
    if not os.path.exists(pdf_path):
        print(f"❌ No se encuentra el origen: {pdf_path}")
        return

    print("🚀 Procesando e Indexando el Padrón San Ignacio 2026 por Bloques...")
    
    # Regex elástico: Orden, Cédula (con comas), Nombre/Apellido (Texto), y las dos fechas finales (Nacimiento y Afiliación)
    re_elector = re.compile(r"^(\d+)\s+([\d,.]+)\s+(.+?)\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})$")
    
    all_records = []
    current_local = "NO ESPECIFICADO"
    current_mesa = 0

    with pdfplumber.open(pdf_path) as pdf:
        total_paginas = len(pdf.pages)
        
        for idx in range(total_paginas):
            page = pdf.pages[idx]
            texto = page.extract_text()
            if not texto:
                continue
                
            lineas = [l.strip() for l in texto.split("\n") if l.strip()]
            
            # Captura dinámica de metadatos de cabecera por página
            for l in lineas[:12]:
                l_upper = l.upper()
                if "MESA" in l_upper:
                    match_mesa = re.search(r"MESA\s+(\d+)", l, re.IGNORECASE)
                    if match_mesa:
                        current_mesa = int(match_mesa.group(1))
                if "LOCAL" in l_upper:
                    current_local = l.replace("Local", "").replace("LOCAL", "").strip()

            # Extracción atómica de electores
            for linea in lineas:
                # Limpiamos potenciales ruidos marginales
                match = re_elector.match(linea)
                if match:
                    try:
                        orden = int(match.group(1))
                        cedula = int(match.group(2).replace(",", "").replace(".", ""))
                        nombre_completo = match.group(3).strip().upper()
                        fecha_nac = match.group(4).strip()
                        fecha_afi = match.group(5).strip()
                        
                        all_records.append({
                            "local_votacion": current_local,
                            "mesa": current_mesa,
                            "orden_mesa": orden,
                            "cedula": cedula,
                            "nombre_completo": nombre_completo,
                            "fecha_nacimiento": fecha_nac,
                            "fecha_afiliacion": fecha_afi
                        })
                    except Exception:
                        continue
            
            page.flush_cache()
            if idx % 50 == 0 and idx > 0:
                print(f"⏳ Páginas procesadas: {idx}/{total_paginas}...")
                gc.collect()

    if not all_records:
        print("⚠️ Alerta: El regex no capturó filas. Revisar alineación estructural.")
        return

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=["cedula"]) # Un elector, un registro único en el padrón
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"✅ Ingesta terminada. {len(df)} electores reales blindados en Silver Parquet para San Ignacio.")

if __name__ == "__main__":
    parse_padron_2026()
