import os
import re
import pdfplumber
import pandas as pd

def parse_cargo_regional(nombre_archivo, cargo_nombre):
    pdf_path = os.path.expanduser(f"~/projects/electoral_os/data/1_bronze/tsje_resumenes_oficiales_2023/{nombre_archivo}")
    output_path = os.path.expanduser(f"~/projects/electoral_os/data/2_silver/silver_tsje_resumen_{cargo_nombre}_2023.parquet")
    
    if not os.path.exists(pdf_path):
        print(f"❌ No se encuentra el origen: {pdf_path}")
        return

    print(f"🚀 Iniciando procesamiento dinámico de {nombre_archivo}...")
    
    re_depto = re.compile(r"Departamento\s*:\s*(.+)")
    re_votos = re.compile(r"^(\d+)\s+(.+?)\s+([\d.,]+)$")
    
    records = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if not texto:
                continue
                
            lineas = texto.split("\n")
            current_depto = None
            
            for linea in lineas:
                linea = linea.strip()
                
                if any(x in linea for x in ["RESUMEN GENERAL", "TOTALES", "CUADRO COMPARATIVO"]):
                    current_depto = None
                    continue
                
                match_depto = re_depto.search(linea)
                if match_depto:
                    current_depto = match_depto.group(1).strip().upper()
                    continue
                
                match_votos = re_votos.match(linea)
                if match_votos and current_depto:
                    num_lista = int(match_votos.group(1))
                    partido = match_votos.group(2).strip().upper()
                    votos_str = match_votos.group(3).replace(",", "").replace(".", "")
                    
                    if any(x in partido for x in ["TOTAL", "VALIDOS", "BLANCOS", "NULOS", "EMITIDOS", "PARTICIPACION"]):
                        continue
                        
                    if len(partido) < 3:
                        continue
                        
                    votos = float(votos_str)
                    
                    records.append({
                        "departamento": current_depto,
                        "lista": num_lista,
                        "partido": partido,
                        "votos": votos
                    })

    df = pd.DataFrame(records)
    df = df.drop_duplicates()
    df = df.dropna(subset=["departamento"])
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"✅ Concluido. {len(df)} registros guardados en: {output_path}\n")

if __name__ == "__main__":
    # Procesamos Gobernadores
    parse_cargo_regional("GOBERNADORES_2023.pdf", "gobernadores")
    # Procesamos Juntas Departamentales (Concejales)
    parse_cargo_regional("JUNTA_DEPARTAMENTAL_2023.pdf", "concejales")
