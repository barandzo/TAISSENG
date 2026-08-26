"""
API TAISS Retail - indicateurs metier issus du Data Warehouse.

Chaque endpoint lit directement le modele en etoile : les mesures sont
deja pre-calculees dans fact_sales, l API se contente d agreger.
"""
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from db import query, query_one

app = FastAPI(
    title="TAISS Retail API",
    description="Indicateurs de ventes issus du Data Warehouse",
    version="1.0.0",
)


@app.get("/health", tags=["technique"])
def health():
    """Verifie que l API et la base repondent."""
    try:
        query_one("SELECT 1 AS ok")
        return {"status": "ok", "database": "reachable"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Base indisponible: " + str(exc))


@app.get("/sales/summary", tags=["indicateurs"])
def sales_summary(
    date_from: Optional[str] = Query(None, description="AAAA-MM-JJ"),
    date_to: Optional[str] = Query(None, description="AAAA-MM-JJ"),
):
    """Synthese globale : chiffre d affaires, volume, panier moyen, top produit."""
    filtre, params = _filtre_periode(date_from, date_to)

    glob = query_one("""
        SELECT COALESCE(SUM(f.total_amount), 0)  AS total_sales,
               COUNT(*)                          AS transactions,
               COALESCE(SUM(f.quantity), 0)      AS units_sold,
               COALESCE(ROUND(AVG(f.total_amount), 2), 0) AS average_basket
        FROM fact_sales f
        JOIN dim_date d ON d.date_key = f.date_key
    """ + filtre, params)

    top = query_one("""
        SELECT p.product_name, SUM(f.total_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date d    ON d.date_key    = f.date_key
        JOIN dim_product p ON p.product_key = f.product_key
    """ + filtre + """
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT 1
    """, params)

    return {
        "total_sales": float(glob["total_sales"]),
        "transactions": glob["transactions"],
        "units_sold": glob["units_sold"],
        "average_basket": float(glob["average_basket"]),
        "best_selling_product": top["product_name"] if top else None,
        "period": {"from": date_from, "to": date_to},
    }


@app.get("/sales/by-store", tags=["indicateurs"])
def sales_by_store(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    """Chiffre d affaires par point de vente, du plus fort au plus faible."""
    filtre, params = _filtre_periode(date_from, date_to)
    rows = query("""
        SELECT s.store_id,
               s.store_name,
               s.city,
               COUNT(*)              AS transactions,
               SUM(f.quantity)       AS units_sold,
               SUM(f.total_amount)   AS revenue
        FROM fact_sales f
        JOIN dim_date d  ON d.date_key  = f.date_key
        JOIN dim_store s ON s.store_key = f.store_key
    """ + filtre + """
        GROUP BY s.store_id, s.store_name, s.city
        ORDER BY revenue DESC
    """, params)
    return {"count": len(rows), "results": _floats(rows, "revenue")}


@app.get("/sales/by-product", tags=["indicateurs"])
def sales_by_product(
    limit: int = Query(10, ge=1, le=100),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    """Top des produits par chiffre d affaires."""
    filtre, params = _filtre_periode(date_from, date_to)
    params["limit"] = limit
    rows = query("""
        SELECT p.product_id,
               p.product_name,
               p.category,
               SUM(f.quantity)     AS units_sold,
               SUM(f.total_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date d    ON d.date_key    = f.date_key
        JOIN dim_product p ON p.product_key = f.product_key
    """ + filtre + """
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY revenue DESC
        LIMIT :limit
    """, params)
    return {"count": len(rows), "results": _floats(rows, "revenue")}


@app.get("/sales/daily", tags=["indicateurs"])
def sales_daily():
    """Evolution quotidienne du chiffre d affaires."""
    rows = query("""
        SELECT d.full_date::text AS date,
               d.day_name,
               COUNT(*)            AS transactions,
               SUM(f.total_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date d ON d.date_key = f.date_key
        GROUP BY d.full_date, d.day_name
        ORDER BY d.full_date
    """)
    return {"count": len(rows), "results": _floats(rows, "revenue")}


@app.get("/sales/by-category", tags=["indicateurs"])
def sales_by_category():
    """Repartition du chiffre d affaires par categorie de produit."""
    rows = query("""
        SELECT p.category,
               SUM(f.quantity)     AS units_sold,
               SUM(f.total_amount) AS revenue,
               ROUND(100.0 * SUM(f.total_amount)
                     / NULLIF((SELECT SUM(total_amount) FROM fact_sales), 0), 2) AS share_pct
        FROM fact_sales f
        JOIN dim_product p ON p.product_key = f.product_key
        GROUP BY p.category
        ORDER BY revenue DESC
    """)
    return {"count": len(rows), "results": _floats(rows, "revenue", "share_pct")}


@app.get("/quality/report", tags=["qualite"])
def quality_report():
    """Rapport de qualite : rejets par motif et journal des fichiers traites."""
    rejets = query("""
        SELECT reject_reason, COUNT(*) AS count
        FROM rejected_transactions
        GROUP BY reject_reason
        ORDER BY count DESC
    """)
    fichiers = query("""
        SELECT file_name, rows_read, rows_loaded, rows_rejected,
               status, processed_at::text AS processed_at
        FROM etl_file_log
        ORDER BY file_name
    """)
    charges = query_one("SELECT COUNT(*) AS n FROM fact_sales")
    rejetes = query_one("SELECT COUNT(*) AS n FROM rejected_transactions")

    total = charges["n"] + rejetes["n"]
    return {
        "rows_loaded": charges["n"],
        "rows_rejected": rejetes["n"],
        "acceptance_rate_pct": round(100.0 * charges["n"] / total, 2) if total else None,
        "rejects_by_reason": rejets,
        "files": fichiers,
    }


# ------------------------------------------------------------------ helpers
def _filtre_periode(date_from, date_to):
    """Construit la clause WHERE sur la periode. Requetes parametrees
    uniquement : aucune concatenation de valeur utilisateur en SQL."""
    conditions, params = [], {}
    if date_from:
        conditions.append("d.full_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("d.full_date <= :date_to")
        params["date_to"] = date_to
    clause = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return clause, params


def _floats(rows, *colonnes):
    """Convertit les Decimal PostgreSQL en float pour la serialisation JSON."""
    for r in rows:
        for c in colonnes:
            if r.get(c) is not None:
                r[c] = float(r[c])
    return rows