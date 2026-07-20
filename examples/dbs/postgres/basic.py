from __future__ import annotations

from DBDuck import UDOM


def main() -> None:
    # Update credentials/host/database for your local PostgreSQL.
    url = "postgresql+psycopg2://username:pass@localhost:5432/dbduck"
    db = UDOM(db_type="sql", db_instance="postgres", url=url)

    print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
    print(db.find("Orders", where={"paid": True}, limit=10))


if __name__ == "__main__":
    main()
