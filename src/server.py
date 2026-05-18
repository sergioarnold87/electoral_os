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
    # Enforzamos CAST a VARCHAR en filtros para neutralizar desalineación de tipos en 'ano'
    if ano:
        where_clauses.append(f"CAST(ano AS VARCHAR) = '{ano}'")
    if departamento:
        where_clauses.append(f"UPPER(TRIM(departamento)) = '{departamento.upper().strip()}'")
    if distrito:
        where_clauses.append(f"UPPER(TRIM(distrito)) = '{distrito.upper().strip()}'")
        
    where_stmt = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    # query analítica con TRY_CAST defensivo para ignorar basura alfabética en votos
    query = f"""
        SELECT 
            CAST(ano AS VARCHAR) AS ano,
            COALESCE(categoria, 'GENERAL') AS categoria,
            UPPER(TRIM(departamento)) AS departamento,
            UPPER(TRIM(distrito)) AS distrito,
            UPPER(TRIM(partido)) AS partido,
            SUM(TRY_CAST(votos AS BIGINT)) AS total_votos
        FROM read_parquet('{PATH_SILVER}', union_by_name=True)
        {where_stmt}
        GROUP BY 1, 2, 3, 4, 5
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