"""Application entrypoint for the full stack DBDuck showcase app."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from DBDuck.core.exceptions import ConnectionError, QueryError, TransactionError

from .core.config import (
    get_int_env,
    get_jwt_secret,
    get_payment_gateway_key_id,
    get_payment_gateway_key_secret,
)
from .api import router as api_router
from .db import bind_models, bootstrap_schema, build_db, seed_demo_data
from .web import router as web_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = build_db()
    bind_models(db)
    bootstrap_schema(db)
    seed_demo_data()
    app.state.db = db
    app.state.session_ttl_seconds = get_int_env("APP_SESSION_TTL_SECONDS", 3600)
    app.state.jwt_secret = get_jwt_secret()
    app.state.payment_gateway_key_id = get_payment_gateway_key_id()
    app.state.payment_gateway_key_secret = get_payment_gateway_key_secret()
    try:
        yield
    finally:
        db.close()


app = FastAPI(
    title="DBDuck Full Stack Showcase",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(QueryError)
@app.exception_handler(ConnectionError)
@app.exception_handler(TransactionError)
async def dbduck_error_handler(_request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc)},
    )


app.include_router(web_router)
app.include_router(api_router)
