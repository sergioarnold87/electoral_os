# Electoral OS (Paraguay)

Engine analítico de alta densidad y baja latencia para la ingesta, normalización y servicio de activos de datos electorales históricos del Tribunal Superior de Justicia Electoral (TSJE) de Paraguay.

## 🏗️ Arquitectura de Datos (Medallion Pattern)

La plataforma implementa un Lakehouse local optimizado para FinOps, capaz de procesar e indexar más de 37 millones de registros atómicos directamente en hardware local sin costo operativo de infraestructura cloud:

* **Bronze Layer (Ingesta Cruda):** Extractor idempotente conectado al catálogo universal DKAN (`data.json`). Implementa ruteo defensivo por firma binaria (.csv/.xlsx) e inyección de códec `LATIN1` para neutralizar fallas de unicode de origen.
* **Silver Layer (Normalización):** Motor de transposición matricial masiva (`UNPIVOT`) desarrollado sobre DuckDB. Neutraliza el *Schema Drift* horizontal interanual, convirtiendo layouts anchos de partidos políticos en un formato largo (*Long Format*) unificado.
* **Gold Layer (Gobernanza):** Tablas maestras de dimensiones analíticas (`dim_geografia_maestra`, `dim_partidos_maestros`). Aplica reglas de *Entity Resolution* mediante acoplamiento explícito por nombre (`union_by_name=True`), aislando la fragmentación tipográfica histórica del portal público.
* **Service Layer (API Engine):** Capa de servicio de alta velocidad con FastAPI y Uvicorn ASGI, ejecutando agregaciones vectoriales complejas en memoria en milisegundos a través de un pool de DuckDB embebido.

## 🛠️ Tech Stack

* **Engine Analítico:** DuckDB (Motor analítico columnar vectorial in-memory)
* **Formato de Almacenamiento:** Apache Parquet (Compresión Snappy por bloque)
* **Framework de Servicio:** FastAPI / Uvicorn ASGI
* **Procesamiento y Parsing:** Pandas / OpenPyXL
* **Sistema Operativo Base:** Linux Mint (HP ProBook Architecture)

## 🚀 Instalación y Ejecución

```bash
# 1. Clonar el repositorio de infraestructura
git clone [https://github.com/](https://github.com/)<TU_USUARIO>/electoral_os.git
cd electoral_os

# 2. Inicializar el entorno virtual e instalar el manifiesto de dependencias
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Orquestar el pipeline completo (Bronze -> Silver -> Gold)
python src/ingest/pipeline_tsje.py
python src/transform/normalizer_tsje.py
python src/transform/build_gold_catalogs.py

# 4. Inicializar el motor de la API Analítica
python -m uvicorn src.server:app --reload --host 0.0.0.0 --port 8000