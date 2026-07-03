Installation
============

DBDuck 0.4.1 supports Python 3.10 and newer.

Base package
------------

The base package includes SQLite support and the common SQL abstraction:

.. code-block:: bash

   pip install dbduck

Backend extras
--------------

Install only the drivers required by your application:

.. code-block:: bash

   pip install "dbduck[mysql]"
   pip install "dbduck[postgres]"
   pip install "dbduck[mssql]"
   pip install "dbduck[sql]"       # all synchronous SQL drivers
   pip install "dbduck[mongo]"
   pip install "dbduck[graph]"
   pip install "dbduck[vector]"
   pip install "dbduck[async]"
   pip install "dbduck[all]"

Verify the installation
-----------------------

.. code-block:: bash

   dbduck version

Or from Python:

.. code-block:: python

   import DBDuck

   print(DBDuck.__version__)
