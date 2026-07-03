SQL Migrations
==============

DBDuck generates Alembic migrations from ``UModel`` declarations.

.. code-block:: bash

   # PowerShell
   $env:DATABASE_URL="sqlite:///app.db"

   dbduck makemigrations --module models --message "create users"
   dbduck migrate --direction up

Use ``--project-dir`` when the model module belongs to a different project
directory. Commit the generated ``migrations/sql/`` directory to version
control.

SQLite alterations
------------------

DBDuck 0.4.1 enables Alembic batch mode for SQLite. Schema changes that SQLite
cannot execute with a direct ``ALTER TABLE`` are applied by creating the new
table layout and copying existing rows into it.

Review generated revisions before applying them and back up production data.

Rollback
--------

.. code-block:: bash

   dbduck migrate --direction down
