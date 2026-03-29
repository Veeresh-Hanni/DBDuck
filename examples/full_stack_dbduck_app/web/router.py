"""Web routes for the full stack DBDuck showcase app."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from .views import HOME_HTML


router = APIRouter(tags=["web"])


@router.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    return HTMLResponse(HOME_HTML)
