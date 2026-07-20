from __future__ import annotations

from urllib.parse import quote_plus

from DBDuck import UDOM


def main() -> None:
    # Update credentials/server/database for your local SQL Server.
    odbc_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=servername;"
        "DATABASE=dbduck;"
        "UID=username;"
        "PWD=pass;"
        "TrustServerCertificate=yes;"
    )
    url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
    db = UDOM(db_type="sql", db_instance="mssql", url=url)

    db.create_many(
        "Orders",
        [
            {"order_id": 501, "customer": "A", "paid": True},
            {"order_id": 502, "customer": "B", "paid": False},
            {"order_id": 503, "customer": "C", "paid": True},
        ],
    )

    with db.transaction():
        db.update("Orders", data={"paid": True}, where={"order_id": 502})

    print("count_paid:", db.count("Orders", where={"paid": True}))
    print("page_1:", db.find_page("Orders", page=1, page_size=2, order_by="order_id ASC"))
    print("delete_result:", db.delete("Orders", where={"order_id": 503}))
    print("final_rows:", db.find("Orders", order_by="order_id ASC"))


if __name__ == "__main__":
    main()
