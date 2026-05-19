import os
import glob
import pdfplumber

def analizar_paginas_datos():
    ruta_anr = os.path.expanduser("~/projects/electoral_os/data/1_bronze/anr/*.pdf")
    archivos = glob.glob(ruta_anr)
    
    if not archivos:
        print("❌ ERROR: No se encontraron archivos .pdf")
        return

    pdf_path = archivos[0]
    print(f"📄 Abriendo: {os.path.basename(pdf_path)} para inspección de datos...")

    with pdfplumber.open(pdf_path) as pdf:
        total_paginas = len(pdf.pages)
        print(f"📊 Total de páginas del informe: {total_paginas}")
        
        # Escaneamos desde la página 2 hasta la 5 (índices 1 a 4) para localizar tablas
        for i in range(1, min(5, total_paginas)):
            page = pdf.pages[i]
            print(f"\n--- INSPECCIONANDO PÁGINA {i + 1} ---")
            
            # 1. Probar si el motor detecta tablas dibujadas con líneas vectoriales
            tables = page.extract_tables()
            if tables:
                print(f"✅ Se detectaron {len(tables)} tablas estructuradas en esta página.")
                print("Muestra de la primera tabla detectada (Primeras 3 filas):")
                for row in tables[0][:3]:
                    print(row)
            else:
                print("⚠️ No se detectaron tablas vectoriales automáticas. Evaluando texto plano...")
                texto = page.extract_text()
                if texto:
                    # Mostramos las primeras líneas de texto para ver la alineación
                    lineas = texto.split("\n")
                    for linea in lineas[:10]:
                        print(linea)
                else:
                    print("Página vacía o ilegible.")

if __name__ == "__main__":
    analizar_paginas_datos()