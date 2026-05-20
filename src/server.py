import os
import duckdb
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Electoral OS - Data API",
    description="Engine analítico de alta densidad para el dominio electoral y geomarketing de Paraguay.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PATH_SILVER = os.path.expanduser("~/projects/electoral_os/data/2_silver/*.parquet")
PATH_GOLD_GEO = os.path.expanduser("~/projects/electoral_os/data/3_gold/dim_geografia_maestra.parquet")
PATH_GOLD_PARTIDOS = os.path.expanduser("~/projects/electoral_os/data/3_gold/dim_partidos_maestros.parquet")

@app.get("/api/v1/geografia")
def obtener_geografia():
    con = duckdb.connect(database=':memory:')
    try:
        query = f"SELECT departamento_raw AS departamento, distrito_raw AS distrito FROM read_parquet('{PATH_GOLD_GEO}')"
        df = con.execute(query).df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()

@app.get("/api/v1/partidos")
def obtener_partidos():
    con = duckdb.connect(database=':memory:')
    try:
        query = f"SELECT partido_raw AS partido, volumen_menciones FROM read_parquet('{PATH_GOLD_PARTIDOS}') WHERE volumen_menciones > 10"
        df = con.execute(query).df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()

@app.get("/api/v1/resultados/agregados")
def obtener_resultados_agregados(
    ano: str = Query(None, description="Año electoral a filtrar"),
    departamento: str = Query(None, description="Departamento geográfico"),
    distrito: str = Query(None, description="Distrito o municipio objetivo")
):
    con = duckdb.connect(database=':memory:')
    
    where_clauses = []
    if ano:
        where_clauses.append(f"ano_str = '{ano}'")
    if departamento:
        where_clauses.append(f"departamento_norm = '{departamento.upper().strip()}'")
    if distrito:
        where_clauses.append(f"distrito_norm = '{distrito.upper().strip()}'")
        
    where_stmt = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    # Engine robusto: Extracción por Regex de metadatos del archivo y COALESCE de esquemas mutados
    query = f"""
        WITH data_preparada AS (
            SELECT 
                REGEXP_EXTRACT(filename, '(\d{4})_\d{4}', 1) AS ano_str,
                CASE 
                    WHEN filename LIKE '%preferentes%' THEN 'PREFERENTES' 
                    ELSE 'GENERAL' 
                END AS categoria_norm,
                UPPER(TRIM(COALESCE(departamento, 'NO ESPECIFICADO'))) AS departamento_norm,
                UPPER(TRIM(COALESCE(distrito, DISTRITO, 'NO ESPECIFICADO'))) AS distrito_norm,
                UPPER(TRIM(COALESCE(partido, 'OTROS'))) AS partido_norm,
                TRY_CAST(votos AS BIGINT) AS votos_num
            FROM read_parquet('{PATH_SILVER}', union_by_name=True, filename=True)
        )
        SELECT 
            ano_str AS ano,
            categoria_norm AS categoria,
            departamento_norm AS departamento,
            distrito_norm AS distrito,
            partido_norm AS partido,
            SUM(votos_num) AS total_votos
        FROM data_preparada
        {where_stmt}
        GROUP BY ALL
        HAVING total_votos > 0
        ORDER BY total_votos DESC
        LIMIT 500
    """
    try:
        df = con.execute(query).df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()