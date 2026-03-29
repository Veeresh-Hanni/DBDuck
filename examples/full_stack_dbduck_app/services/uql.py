"""UQL sample service functions for the full stack DBDuck showcase app."""

from __future__ import annotations

from DBDuck import UDOM


def uql_sample(db: UDOM) -> dict[str, object]:
    return {
        "uquery": db.uquery("FIND orders WHERE paid = true ORDER BY id DESC LIMIT 5"),
        "uexecute": db.uexecute("FIND orders WHERE paid = true ORDER BY id DESC LIMIT 5"),
    }
