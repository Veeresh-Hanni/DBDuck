from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse
import urllib
import uvicorn
from dotenv import load_dotenv
load_dotenv()
from DBDuck import UDOM
from DBDuck.core.exceptions import DatabaseError
from DBDuck.models import Boolean, Column, Integer, String, UModel


class Order(UModel):
    __entity__ = "Orders"
    __strict__ = True
    __sensitive_fields__ = []
    id = Column(Integer, nullable=True)
    order_id = Column(Integer, unique=True)
    customer = Column(String)
    paid = Column(Boolean, default=False)


class OrderCreate(BaseModel):
    order_id: int = Field(..., ge=1)
    customer: str = Field(..., min_length=1, max_length=120)
    paid: bool = False


class OrderUpdate(BaseModel):
    customer: str | None = Field(default=None, min_length=1, max_length=120)
    paid: bool | None = None


class OrderOut(BaseModel):
    id: int | None = None
    order_id: int
    customer: str
    paid: bool


class BulkCreatePayload(BaseModel):
    rows: list[OrderCreate] = Field(..., min_length=1)


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def _build_db() -> UDOM:
    db_user = os.getenv("MYSQL_USER","root")
    db_pass = os.getenv("MYSQL_PASSWORD","password") # Password-nalli symbols idre quote_plus use mad
    db_hostname = os.getenv("MYSQL_HOST","localhost")
    db_name = os.getenv("MYSQL_DB","DBDuck")
    port = os.getenv("MYSQL_PORT",3306)
    safe_pass = urllib.parse.quote_plus(db_pass)
    # 1. Database name illade URL build madi
    # Note: port aamele '/' matra ide, db name illa.
    base_url = f"mysql+pymysql://{db_user}:{safe_pass}@{db_hostname}:{port}/{db_name}"

    db_type = _get_env("APP_DB_TYPE", "sql")
    db_instance = _get_env("APP_DB_INSTANCE", "mysql")
    db_url = _get_env("APP_DB_URL", base_url)

    log_level = _get_env("APP_LOG_LEVEL", "INFO")
    return UDOM(
        db_type=db_type,
        db_instance=db_instance,
        url=db_url,
        log_level=log_level,
        allow_unsafe_where_strings=False,
    )


def _dump_model(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _row_to_order(row: dict[str, Any]) -> OrderOut:
    return OrderOut(
        id=row.get("id"),
        order_id=int(row["order_id"]),
        customer=str(row["customer"]),
        paid=bool(row["paid"]),
    )


def _order_to_out(order: Order) -> OrderOut:
    payload = order.to_dict(include_none=True, only_declared=False)
    return _row_to_order(payload)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = _build_db()
    Order.bind(app.state.db)
    try:
        yield
    finally:
        app.state.db.close()


app = FastAPI(
    title="DBDuck FastAPI Production Example",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(DatabaseError)
async def db_error_handler(_request: Request, exc: DatabaseError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc)},
    )


def _db(request: Request) -> UDOM:
    return request.app.state.db


@app.get("/health")
def health(request: Request):
    try:
        ping = _db(request).ping()
        return {"status": "ok", "db": "connected", "ping": ping}
    except DatabaseError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.post("/orders", response_model=OrderOut, status_code=status.HTTP_201_CREATED)
def create_order(payload: OrderCreate, request: Request):
    db = _db(request)
    Order(**_dump_model(payload)).save(db=db)
    row = Order.find_one(where={"order_id": payload.order_id}, db=db)
    if row is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Insert verification failed")
    return _order_to_out(row)


@app.post("/orders/bulk")
def create_orders_bulk(payload: BulkCreatePayload, request: Request):
    db = _db(request)
    rows = [Order(**_dump_model(r)) for r in payload.rows]
    result = Order.bulk_create(rows, db=db)
    return {"created": len(payload.rows), "result": result}


@app.get("/orders", response_model=list[OrderOut])
def list_orders(
    request: Request,
    paid: bool | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
):
    db = _db(request)
    where = {"paid": paid} if paid is not None else None
    page_data = Order.find_page(page=page, page_size=page_size, where=where, order_by="order_id ASC", db=db)
    return [_order_to_out(item) for item in page_data["items"]]


@app.get("/orders/{order_id}", response_model=OrderOut)
def get_order(order_id: int, request: Request):
    db = _db(request)
    row = Order.find_one(where={"order_id": order_id}, db=db)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    return _order_to_out(row)


@app.patch("/orders/{order_id}", response_model=OrderOut)
def update_order(order_id: int, payload: OrderUpdate, request: Request):
    db = _db(request)
    data = {k: v for k, v in _dump_model(payload).items() if v is not None}
    if not data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No update fields provided")
    row = Order.find_one(where={"order_id": order_id}, db=db)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    row.update(data=data, where={"order_id": order_id}, db=db)
    refreshed = Order.find_one(where={"order_id": order_id}, db=db)
    if refreshed is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    return _order_to_out(refreshed)


@app.delete("/orders/{order_id}")
def delete_order(order_id: int, request: Request):
    db = _db(request)
    row = Order.find_one(where={"order_id": order_id}, db=db)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    result = row.delete(where={"order_id": order_id}, db=db)
    return {"deleted_order_id": order_id, "result": result}


@app.get("/orders/stats")
def order_stats(request: Request):
    db = _db(request)
    return {
        "by_paid": Order.aggregate(
            group_by="paid",
            metrics={"total_orders": "count(*)"},
            order_by="paid DESC",
            db=db,
        )
    }

if __name__ == "__main__":
    uvicorn.run(app=app)