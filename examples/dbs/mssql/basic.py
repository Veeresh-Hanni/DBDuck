from __future__ import annotations

from urllib.parse import quote_plus

from DBDuck import UDOM


def main() -> None:
    # Update credentials/server/database for your local SQL Server.
    odbc_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=dbduck;"
        "UID=username;"
        "PWD=pass;"
        "TrustServerCertificate=yes;"
    )
    url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
    db = UDOM(db_type="sql", db_instance="mssql", url=url)

    print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
    print(db.find("Orders", where={"paid": True}, limit=10))


if __name__ == "__main__":
    main()
