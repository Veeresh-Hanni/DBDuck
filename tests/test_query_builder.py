"""Tests for QueryBuilder fluent DSL."""

from pathlib import Path

import pytest

from DBDuck import UDOM, QueryBuilder


@pytest.fixture
def db(tmp_path: Path):
    """Create a test SQLite database."""
    db_path = tmp_path / "query_builder.db"
    udom = UDOM(url=f"sqlite:///{db_path.as_posix()}")
    udom.adapter.run_native("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            role TEXT,
            active INTEGER DEFAULT 1,
            age INTEGER
        )
    """)
    udom.adapter.run_native("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            status TEXT
        )
    """)
    # Insert test data
    udom.create("users", {"id": 1, "name": "Alice", "email": "alice@test.com", "role": "admin", "active": 1, "age": 30})
    udom.create("users", {"id": 2, "name": "Bob", "email": "bob@test.com", "role": "user", "active": 1, "age": 25})
    udom.create("users", {"id": 3, "name": "Charlie", "email": "charlie@test.com", "role": "user", "active": 0, "age": 35})
    udom.create("users", {"id": 4, "name": "Diana", "email": "diana@test.com", "role": "admin", "active": 1, "age": 28})
    
    udom.create("orders", {"id": 1, "user_id": 1, "amount": 100.0, "status": "completed"})
    udom.create("orders", {"id": 2, "user_id": 1, "amount": 50.0, "status": "pending"})
    udom.create("orders", {"id": 3, "user_id": 2, "amount": 75.0, "status": "completed"})
    return udom


class TestQueryBuilderBasics:
    """Basic QueryBuilder functionality tests."""

    def test_table_returns_query_builder(self, db):
        """table() should return a QueryBuilder instance."""
        qb = db.table("users")
        assert isinstance(qb, QueryBuilder)

    def test_find_all(self, db):
        """find() without conditions returns all records."""
        users = db.table("users").find()
        assert len(users) == 4

    def test_find_with_where_kwargs(self, db):
        """where() with kwargs filters records."""
        users = db.table("users").where(role="admin").find()
        assert len(users) == 2
        assert all(u["role"] == "admin" for u in users)

    def test_find_with_where_dict(self, db):
        """where() with dict filters records."""
        users = db.table("users").where({"active": 1}).find()
        assert len(users) == 3

    def test_find_with_multiple_where(self, db):
        """Multiple where() calls are ANDed together."""
        users = db.table("users").where(role="admin").where(active=1).find()
        assert len(users) == 2
        assert all(u["role"] == "admin" and u["active"] == 1 for u in users)

    def test_first_returns_single_record(self, db):
        """first() returns single record or None."""
        user = db.table("users").where(id=1).first()
        assert user is not None
        assert user["name"] == "Alice"

    def test_first_returns_none_when_not_found(self, db):
        """first() returns None when no match."""
        user = db.table("users").where(id=999).first()
        assert user is None

    def test_count(self, db):
        """count() returns number of matching records."""
        count = db.table("users").where(active=1).count()
        assert count == 3

    def test_exists_true(self, db):
        """exists() returns True when records match."""
        assert db.table("users").where(name="Alice").exists() is True

    def test_exists_false(self, db):
        """exists() returns False when no records match."""
        assert db.table("users").where(name="NonExistent").exists() is False


class TestQueryBuilderChaining:
    """Test method chaining."""

    def test_order_asc(self, db):
        """order() with ASC."""
        users = db.table("users").where(active=1).order("name", "ASC").find()
        names = [u["name"] for u in users]
        assert names == sorted(names)

    def test_order_desc(self, db):
        """order() with DESC."""
        users = db.table("users").where(active=1).order("age", "DESC").find()
        ages = [u["age"] for u in users]
        assert ages == sorted(ages, reverse=True)

    def test_order_by_string(self, db):
        """order_by() with raw string."""
        users = db.table("users").order_by("name DESC").limit(2).find()
        assert len(users) == 2

    def test_limit(self, db):
        """limit() restricts results."""
        users = db.table("users").limit(2).find()
        assert len(users) == 2

    def test_select_fields(self, db):
        """select() projects specific fields."""
        users = db.table("users").select("id", "name").find()
        assert len(users) > 0
        for user in users:
            assert "id" in user
            assert "name" in user
            assert "email" not in user

    def test_select_with_first(self, db):
        """select() works with first()."""
        user = db.table("users").select("name", "email").where(id=1).first()
        assert user is not None
        assert "name" in user
        assert "email" in user
        assert "role" not in user

    def test_page(self, db):
        """page() sets offset and limit for pagination."""
        page1 = db.table("users").order("id").page(1, 2).find()
        page2 = db.table("users").order("id").page(2, 2).find()
        
        assert len(page1) == 2
        assert len(page2) == 2
        assert page1[0]["id"] != page2[0]["id"]

    def test_full_chain(self, db):
        """Full method chain works."""
        users = (
            db.table("users")
            .select("id", "name", "role")
            .where(active=1)
            .order("name")
            .limit(2)
            .find()
        )
        assert len(users) == 2
        for user in users:
            assert "id" in user
            assert "name" in user


class TestQueryBuilderMutations:
    """Test create, update, delete operations."""

    def test_create(self, db):
        """create() inserts a new record."""
        result = db.table("users").create({
            "id": 10,
            "name": "Eve",
            "email": "eve@test.com",
            "role": "user",
            "active": 1,
            "age": 22
        })
        assert result is not None
        
        user = db.table("users").where(id=10).first()
        assert user is not None
        assert user["name"] == "Eve"

    def test_create_many(self, db):
        """create_many() inserts multiple records."""
        db.table("users").create_many([
            {"id": 20, "name": "Frank", "email": "frank@test.com", "role": "user", "active": 1, "age": 40},
            {"id": 21, "name": "Grace", "email": "grace@test.com", "role": "user", "active": 1, "age": 45},
        ])
        
        assert db.table("users").where(id=20).exists()
        assert db.table("users").where(id=21).exists()

    def test_update(self, db):
        """update() modifies matching records."""
        db.table("users").where(id=1).update({"name": "Alice Updated"})
        
        user = db.table("users").where(id=1).first()
        assert user["name"] == "Alice Updated"

    def test_update_requires_where(self, db):
        """update() without where raises error."""
        with pytest.raises(ValueError, match="update requires"):
            db.table("users").update({"name": "Test"})

    def test_delete(self, db):
        """delete() removes matching records."""
        db.table("users").where(id=3).delete()
        
        assert db.table("users").where(id=3).exists() is False

    def test_delete_requires_where(self, db):
        """delete() without where raises error."""
        with pytest.raises(ValueError, match="delete requires"):
            db.table("users").delete()


class TestQueryBuilderAdvanced:
    """Advanced query builder features."""

    def test_clone(self, db):
        """clone() creates independent copy."""
        base = db.table("users").where(active=1)
        admins = base.clone().where(role="admin").find()
        regular = base.clone().where(role="user").find()
        
        assert len(admins) == 2
        assert len(regular) == 1

    def test_to_dict(self, db):
        """to_dict() returns query state."""
        state = db.table("users").where(active=1).order("name").limit(10).to_dict()
        
        assert state["entity"] == "users"
        assert state["where"] == {"active": 1}
        assert state["order_by"] == "name"
        assert state["limit"] == 10

    def test_repr(self, db):
        """__repr__ returns readable string."""
        qb = db.table("users").where(active=1).limit(10)
        repr_str = repr(qb)
        
        assert "QueryBuilder" in repr_str
        assert "users" in repr_str

    def test_find_page(self, db):
        """find_page() returns paginated results."""
        result = db.table("users").where(active=1).find_page(page=1, page_size=2)
        
        assert "items" in result
        assert "page" in result
        assert "page_size" in result
        assert "total" in result
        assert len(result["items"]) <= 2

    def test_join_state_export(self, db):
        """to_dict() includes join metadata."""
        state = db.table("users").join("orders", on=("id", "user_id")).to_dict()
        assert state["joins"] == [{"entity": "orders", "on": [("id", "user_id")], "type": "inner"}]


class TestQueryBuilderValidation:
    """Test input validation."""

    def test_limit_must_be_positive(self, db):
        """limit() requires positive integer."""
        with pytest.raises(ValueError):
            db.table("users").limit(0)
        
        with pytest.raises(ValueError):
            db.table("users").limit(-1)

    def test_offset_must_be_non_negative(self, db):
        """offset() requires non-negative integer."""
        with pytest.raises(ValueError):
            db.table("users").offset(-1)

    def test_order_direction_must_be_valid(self, db):
        """order() direction must be ASC or DESC."""
        with pytest.raises(ValueError):
            db.table("users").order("name", "INVALID")

    def test_page_must_be_positive(self, db):
        """page() requires positive page number."""
        with pytest.raises(ValueError):
            db.table("users").page(0, 10)

    def test_where_in_requires_list(self, db):
        """where_in() requires list or tuple."""
        with pytest.raises(ValueError):
            db.table("users").where_in("id", "not_a_list")


class TestQueryBuilderExport:
    """Test that QueryBuilder is properly exported."""

    def test_import_from_dbduck(self):
        """QueryBuilder can be imported from DBDuck."""
        from DBDuck import QueryBuilder
        assert QueryBuilder is not None

    def test_import_from_udom(self):
        """QueryBuilder can be imported from DBDuck.udom."""
        from DBDuck.udom import QueryBuilder
        assert QueryBuilder is not None


class TestQueryBuilderOperators:
    """Test comparison operators."""

    def test_where_not(self, db):
        """where_not() excludes matching records."""
        users = db.table("users").where_not(role="admin").find()
        assert all(u["role"] != "admin" for u in users)

    def test_where_null(self, db):
        """where_null() checks for NULL values."""
        # This would need a column that can be NULL
        qb = db.table("users").where_null("email")
        # Just verify it builds without error
        assert qb is not None

    def test_where_not_null(self, db):
        """where_not_null() checks for NOT NULL values."""
        qb = db.table("users").where_not_null("email")
        assert qb is not None

    def test_where_like(self, db):
        """where_like() adds LIKE pattern."""
        qb = db.table("users").where_like(name="%Alice%")
        assert qb is not None

    def test_where_gt(self, db):
        """where_gt() adds greater-than condition."""
        qb = db.table("users").where_gt(age=25)
        state = qb.to_dict()
        assert "age__gt" in state["where"]

    def test_where_gte(self, db):
        """where_gte() adds greater-than-or-equal condition."""
        qb = db.table("users").where_gte(age=25)
        state = qb.to_dict()
        assert "age__gte" in state["where"]

    def test_where_lt(self, db):
        """where_lt() adds less-than condition."""
        qb = db.table("users").where_lt(age=30)
        state = qb.to_dict()
        assert "age__lt" in state["where"]

    def test_where_lte(self, db):
        """where_lte() adds less-than-or-equal condition."""
        qb = db.table("users").where_lte(age=30)
        state = qb.to_dict()
        assert "age__lte" in state["where"]

    def test_where_in(self, db):
        """where_in() adds IN condition."""
        qb = db.table("users").where_in("id", [1, 2, 3])
        state = qb.to_dict()
        assert "id__in" in state["where"]
        assert state["where"]["id__in"] == [1, 2, 3]

    def test_where_lt_executes(self, db):
        """where_lt() executes and filters records."""
        users = db.table("users").where_lt(age=30).order("age", "ASC").find()
        assert [user["name"] for user in users] == ["Bob", "Diana"]

    def test_where_gt_executes(self, db):
        """where_gt() executes and filters records."""
        users = db.table("users").where_gt(age=30).find()
        assert [user["name"] for user in users] == ["Charlie"]

    def test_where_in_executes(self, db):
        """where_in() executes and filters records."""
        users = db.table("users").where_in("id", [1, 4]).order("id", "ASC").find()
        assert [user["name"] for user in users] == ["Alice", "Diana"]

    def test_where_or_executes(self, db):
        """where_or() executes OR groups."""
        users = (
            db.table("users")
            .where_or({"role": "admin"}, {"name": "Bob"})
            .order("id", "ASC")
            .find()
        )
        assert [user["name"] for user in users] == ["Alice", "Bob", "Diana"]


class TestQueryBuilderAggregation:
    """Test aggregation methods."""

    def test_group_by_single(self, db):
        """group_by() with single field."""
        qb = db.table("orders").group_by("status")
        state = qb.to_dict()
        assert state["group_by"] == "status"

    def test_group_by_multiple(self, db):
        """group_by() with multiple fields."""
        qb = db.table("orders").group_by("status", "user_id")
        state = qb.to_dict()
        assert state["group_by"] == ["status", "user_id"]

    def test_metrics(self, db):
        """metrics() sets aggregation metrics."""
        qb = db.table("orders").group_by("status").metrics(total="count", avg_amount="avg:amount")
        state = qb.to_dict()
        assert state["metrics"]["total"] == "count"
        assert state["metrics"]["avg_amount"] == "avg:amount"

    def test_having(self, db):
        """having() sets HAVING conditions."""
        qb = db.table("orders").group_by("user_id").having({"count": {"$gt": 1}})
        state = qb.to_dict()
        assert state["having"] is not None


class TestQueryBuilderJoins:
    """SQL join support in QueryBuilder."""

    def test_inner_join_find(self, db):
        """join() returns joined rows for SQL backends."""
        rows = (
            db.table("users")
            .join("orders", on=("id", "user_id"))
            .select("name", "orders.status")
            .order_by("orders.id ASC")
            .find()
        )
        assert rows == [
            {"name": "Alice", "orders.status": "completed"},
            {"name": "Alice", "orders.status": "pending"},
            {"name": "Bob", "orders.status": "completed"},
        ]

    def test_left_join_where_null(self, db):
        """left_join() supports NULL filtering on joined columns."""
        rows = (
            db.table("users")
            .left_join("orders", on=("id", "user_id"))
            .where({"orders.id__null": True})
            .select("name")
            .order("id", "ASC")
            .find()
        )
        assert rows == [{"name": "Charlie"}, {"name": "Diana"}]

    def test_join_count(self, db):
        """count() works on joined queries."""
        total = (
            db.table("users")
            .join("orders", on=("id", "user_id"))
            .where({"orders.status": "completed"})
            .count()
        )
        assert total == 2

    def test_join_first(self, db):
        """first() works on joined queries."""
        row = (
            db.table("users")
            .join("orders", on=("id", "user_id"))
            .select("name", "orders.amount")
            .order_by("orders.id ASC")
            .first()
        )
        assert row == {"name": "Alice", "orders.amount": 100.0}


class TestQueryBuilderOrConditions:
    """Test OR conditions."""

    def test_where_or_single(self, db):
        """where_or() with single condition group."""
        qb = db.table("users").where_or({"role": "admin"})
        state = qb.to_dict()
        assert state["where"] == {"role": "admin"}

    def test_where_or_multiple(self, db):
        """where_or() with multiple condition groups."""
        qb = db.table("users").where_or({"role": "admin"}, {"role": "superuser"})
        state = qb.to_dict()
        assert "$or" in state["where"]

    def test_where_and_or_combined(self, db):
        """where() and where_or() combined."""
        qb = db.table("users").where(active=1).where_or({"role": "admin"}, {"role": "superuser"})
        state = qb.to_dict()
        assert "$and" in state["where"]
