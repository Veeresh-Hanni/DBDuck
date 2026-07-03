Query Builder
=============

The fluent query builder works across DBDuck backends.

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

``UModel.query()`` returns typed model instances. In 0.4.1, serializing a
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
