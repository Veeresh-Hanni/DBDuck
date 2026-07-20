from __future__ import annotations

from DBDuck import UDOM


def main() -> None:
    # Update credentials/host/database for your local MySQL.
    url = "mysql+pymysql://username:pass@localhost:3306/dbduck"
    db = UDOM(db_type="sql", db_instance="mysql", url=url)

    db.create_many(
        "Orders",
        [
            {"order_id": 301, "customer": "A", "paid": True},
            {"order_id": 302, "customer": "B", "paid": False},
            {"order_id": 303, "customer": "C", "paid": True},
        ],
    )

    with db.transaction():
        db.update("Orders", data={"paid": True}, where={"order_id": 302})

    print("count_paid:", db.count("Orders", where={"paid": True}))
    print("page_1:", db.find_page("Orders", page=1, page_size=2, order_by="order_id ASC"))
    print("delete_result:", db.delete("Orders", where={"order_id": 303}))
    print("final_rows:", db.find("Orders", order_by="order_id ASC"))


if __name__ == "__main__":
    main()
