import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def descargar_resumenes_tsje():
    url_objetivo = "https://tsje.gov.py/consulta_candidatos_electos_2023/"
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    capa_bronze = os.path.join(base_dir, "data", "1_bronze", "tsje_resumenes_oficiales_2023")
    os.makedirs(capa_bronze, exist_ok=True)
    
    print(f"📡 Conectando a la plataforma del TSJE...")
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
        respuesta = requests.get(url_objetivo, headers=headers, timeout=15)
        respuesta.raise_for_status()
    except Exception as e:
        print(f"❌ Falló la conexión de red: {e}")
        return

    soup = BeautifulSoup(respuesta.text, 'html.parser')
    enlaces = soup.find_all('a')
    descargas_exitosas = 0

    print("🔍 Escaneando documentos de escrutinio oficial...")
    for enlace in enlaces:
        href = enlace.get('href')
        if not href:
            continue
            
        if any(href.lower().endswith(ext) for ext in ['.pdf', '.xlsx', '.xls']):
            url_archivo = urljoin(url_objetivo, href)
            nombre_archivo = os.path.basename(href)
            ruta_destino = os.path.join(capa_bronze, nombre_archivo)
            
            print(f"📥 Descargando: {nombre_archivo} -> ", end="", flush=True)
            
            try:
                r_file = requests.get(url_archivo, headers=headers, stream=True, timeout=30)
                r_file.raise_for_status()
                
                with open(ruta_destino, 'wb') as f:
                    for chunk in r_file.iter_content(chunk_size=8192):
                        f.write(chunk)
                print("✅ COMPLETADO")
                descargas_exitosas += 1
            except Exception as e:
                print(f"❌ FALLÓ ({e})")

    print(f"\n🏁 Pipeline finalizado. {descargas_exitosas} activos consolidados en {capa_bronze}")

if __name__ == "__main__":
    descargar_resumenes_tsje()
