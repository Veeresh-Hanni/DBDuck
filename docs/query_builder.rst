Query Builder
=============

The fluent query builder provides one chainable API across DBDuck backends.
Core equality filters, ordering, pagination, projection, CRUD terminals, and
backend-specific graph/vector helpers are available through the same
``db.table("entity")`` entry point.

.. code-block:: python

   users = (
       db.table("users")
       .where(active=True)
       .select("id", "name", "age")
       .order("name")
       .limit(20)
       .find()
   )

UModel projections
------------------

``UModel.query()`` returns typed model instances. In 0.4.4, serializing a
projected model preserves the selected columns exactly:

.. code-block:: python

   users = User.query().select("id", "name", "age").find()
   data = [user.to_dict() for user in users]

   # [{"id": 5, "name": "Ganesh", "age": 28}, ...]

Common operations
-----------------

- Filters: ``where()``, ``where_or()``, ``where_in()``, ``where_gt()`` and related lookups.
- Projection: ``select()``.
- Ordering and pagination: ``order()``, ``limit()``, ``offset()``, ``page()`` and ``find_page()``.
- Terminals: ``find()``, ``first()``, ``count()``, ``exists()``, ``update()`` and ``delete()``.

Backend notes
-------------

Advanced lookup suffixes such as ``__gt`` and ``__in`` are fully exercised by
the SQL adapters. MongoDB and graph backends support the shared table API and
their backend-specific helpers, with deeper lookup parity tracked for the next
minor release.
