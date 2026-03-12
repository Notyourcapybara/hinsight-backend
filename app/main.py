from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from .database import get_db
from .queries import (
    get_filter_options,
    get_summary_cards,
    get_factor_suffering_overview,
    get_factor_risk_overview,
    get_condition_suffering_overview,
    get_condition_risk_overview,
    get_factor_condition_suffering,
    get_factor_severity_suffering,
    get_condition_factor_improvement_suffering,
    get_factor_improvement_suffering,
    get_data_explorer_records,
)

app = FastAPI(title="Hsight API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "HInsight backend running"}


@app.get("/api/filter-options")
def filter_options(db: Session = Depends(get_db)):
    return get_filter_options(db)


@app.get("/api/summary-cards")
def summary_cards(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_summary_cards(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/factor-suffering-overview")
def factor_suffering_overview(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_factor_suffering_overview(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/factor-risk-overview")
def factor_risk_overview(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_factor_risk_overview(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/condition-suffering-overview")
def condition_suffering_overview(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_condition_suffering_overview(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/condition-risk-overview")
def condition_risk_overview(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_condition_risk_overview(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/factor-condition-suffering")
def factor_condition_suffering(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_factor_condition_suffering(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/factor-severity-suffering")
def factor_severity_suffering(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_factor_severity_suffering(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/condition-factor-improvement-suffering")
def condition_factor_improvement_suffering(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_condition_factor_improvement_suffering(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/factor-improvement-suffering")
def factor_improvement_suffering(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    factor: str = Query(default=""),
    db: Session = Depends(get_db),
):
    return get_factor_improvement_suffering(
        db=db,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        factor=factor,
    )


@app.get("/api/data-explorer")
def data_explorer(
    search: str = Query(default=""),
    status: str = Query(default=""),
    severity: str = Query(default=""),
    factor: str = Query(default=""),
    condition_factor: str = Query(default=""),
    condition: str = Query(default=""),
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    region: str = Query(default=""),
    tenant: str = Query(default=""),
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db),
):
    return get_data_explorer_records(
        db=db,
        search=search,
        status=status,
        severity=severity,
        factor=factor,
        condition_factor=condition_factor,
        condition=condition,
        start_date=start_date,
        end_date=end_date,
        region=region,
        tenant=tenant,
        limit=limit,
    )