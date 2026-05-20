import os

# Raíz del proyecto electoral_os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Rutas unificadas para el lago de datos
PATH_SILVER = os.path.join(DATA_DIR, "2_silver", "*.parquet")
PATH_GOLD_GEO = os.path.join(DATA_DIR, "3_gold", "dim_geografia_maestra.parquet")
PATH_GOLD_PARTIDOS = os.path.join(DATA_DIR, "3_gold", "dim_partidos_maestros.parquet")