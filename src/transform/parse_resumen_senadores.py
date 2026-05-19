import os
import re
import pdfplumber
import pandas as pd

def procesar_resumen_senadores():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pdf_target = os.path.join(base_dir, "data", "1_bronze", "tsje_resumenes_oficiales_2023", "resumen_deptal_senado.pdf")
    output_silver = os.path.join(base_dir, "data", "2_silver", "silver_tsje_resumen_senadores_2023.parquet")
    
    if not os.path.exists(pdf_target):
        print(f"❌ No se encuentra el archivo base: {pdf_target}")
        return

    patron_dept = re.compile(r'^Departamento\s*:\s*(.+)$')
    # Regex estricto para partidos: Número + Texto + Votos con comas (evita líneas de candidatos de la pág 2)
    patron_partido = re.compile(r'^(\d+)\s+([A-Z\sÑÁÉÍÓÚÚ.]+?)\s+([\d,]+)$')

    dataset = []
    current_dept = "DESCONOCIDO"

    print(f"🚀 Iniciando procesamiento analítico de {os.path.basename(pdf_target)}...")
    
    with pdfplumber.open(pdf_target) as pdf:
        for num_pag, pagina in enumerate(pdf.pages):
            texto = pagina.extract_text()
            if not texto:
                continue
                
            for linea in texto.split('\n'):
                linea = linea.strip()
                
                # 1. Tracking geográfico
                match_dept = patron_dept.match(linea)
                if match_dept:
                    current_dept = match_dept.group(1).strip().upper()
                    
                # 2. Extracción de registros de votación por partido
                match_partido = patron_partido.match(linea)
                if match_partido and current_dept != "DESCONOCIDO":
                    nro, partido, votos = match_partido.groups()
                    partido = partido.strip()
                    
                    # Regla defensiva: Evitar tragar firmas de candidatos largos que pasen el regex
                    if "OPCION" in partido or "OPCIÓN" in partido:
                        continue
                        
                    # Limpieza y cast de tipos nativos para DuckDB
                    votos_int = int(votos.replace(",", ""))
                    
                    dataset.append({
                        "ano": "2023",
                        "cargo": "SENADORES",
                        "departamento": current_dept,
                        "lista": int(nro),
                        "partido": partido,
                        "votos": votos_int
                    })

    # Convertir a estructura tabular y guardar en formato de alta performance
    if dataset:
        df = pd.DataFrame(dataset)
        # Eliminar registros duplicados de totales generales que el PDF a veces repite al final
        df = df.drop_duplicates()
        df.to_parquet(output_silver, index=False)
        print(f"✅ Procesamiento concluido. {len(df)} registros consolidados en {output_silver}")
    else:
        print("⚠️ El pipeline finalizó pero el dataset quedó vacío. Revisar expresiones regulares.")

if __name__ == "__main__":
    procesar_resumen_senadores()
