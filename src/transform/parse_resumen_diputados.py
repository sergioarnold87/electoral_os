import os
import re
import pdfplumber
import pandas as pd

def parse_diputados():
    pdf_path = os.path.expanduser("~/projects/electoral_os/data/1_bronze/tsje_resumenes_oficiales_2023/DIPUTADOS_2023.pdf")
    output_path = os.path.expanduser("~/projects/electoral_os/data/2_silver/silver_tsje_resumen_diputados_2023.parquet")
    
    if not os.path.exists(pdf_path):
        print(f"❌ No se encuentra el origen: {pdf_path}")
        return

    print("🚀 Iniciando procesamiento dinámico blindado de DIPUTADOS_2023.pdf...")
    
    re_depto = re.compile(r"Departamento\s*:\s*(.+)")
    re_votos = re.compile(r"^(\d+)\s+(.+?)\s+([\d.,]+)$")
    
    records = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            texto = page.extract_text()
            if not texto:
                continue
                
            lineas = texto.split("\n")
            current_depto = None  # Reseteamos el departamento al inicio de cada página por seguridad
            
            for linea in lineas:
                linea = linea.strip()
                
                # Ignorar páginas de resúmenes consolidados finales o firmas
                if "RESUMEN GENERAL" in linea or "TOTALES" in linea or "CUADRO COMPARATIVO" in linea:
                    current_depto = None
                    continue
                
                # 1. Detectar Departamento en la página actual
                match_depto = re_depto.search(linea)
                if match_depto:
                    current_depto = match_depto.group(1).strip().upper()
                    continue
                
                # 2. Capturar matriz de datos si el estado está activo en la página
                match_votos = re_votos.match(linea)
                if match_votos and current_depto:
                    num_lista = int(match_votos.group(1))
                    partido = match_votos.group(2).strip().upper()
                    votos_str = match_votos.group(3).replace(",", "").replace(".", "")
                    
                    # Filtros duros para ignorar metadatos del PDF parseados como partidos
                    if any(x in partido for x in ["TOTAL", "VALIDOS", "BLANCOS", "NULOS", "EMITIDOS", "PARTICIPACION"]):
                        continue
                        
                    # Validar longitud mínima de nombre de partido legítimo
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
    
    # Saneamiento final: Si una página no tenía la cabecera departamento, se descarta
    df = df.dropna(subset=["departamento"])
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"✅ Procesamiento concluido con éxito. {len(df)} registros consolidados en Silver.")

if __name__ == "__main__":
    parse_diputados()
