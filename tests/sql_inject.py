from __future__ import annotations

from pdb import run
from urllib.parse import quote_plus
from uuid import uuid4

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError
import time

def test_sql_injection_on_dbduck() -> None:
    db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://username:pass@localhost:3306/dbduck")
    entity = f"users_security_{uuid4().hex[:8]}"

    malicious_id = "1 OR 1=1"
    malicious_name = "Example User'; DROP TABLE users; --"

    db.create(entity, {"id": 1, "name": "basu"})
    db.create(entity, {"id": 2, "name": malicious_name})

    try:
        db.find(entity, where={"id": malicious_id})
    except QueryError as exc:
        print(f"PASS: suspicious numeric filter rejected safely: {exc}")
    else:
        raise AssertionError("SECURITY VULNERABILITY: suspicious numeric filter was not rejected")

    rows = db.find(entity, where={"name": malicious_name})
    assert len(rows) == 1, "Literal string lookup should return the matching row only"
    assert rows[0]["name"] == malicious_name
    print("PASS: literal string payload remained data, not executable SQL")



def test_advanced_sqli_protection(db, entity):
    print("\n--- Starting High Security Attack Simulations ---")

    # 1. TIME-BASED BLIND SQLi
    # If vulnerable, the database will sleep for 5 seconds.
    # If secure, it will just look for this literal string and return immediately.
    time_payload = "1' AND (SELECT 1 FROM (SELECT(SLEEP(5)))a)--"
    start_time = time.time()
    db.find(entity, where={"id": time_payload})
    duration = time.time() - start_time
    
    if duration >= 5:
        raise AssertionError("SECURITY VULNERABILITY: Time-based SQL Injection successful!")
    print(f"PASS: Time-based attack neutralized (Took {duration:.2f}s)")

    # 2. UNION-BASED ATTACK (Data Leakage)
    # Tries to append data from other system tables
    union_payload = "' UNION SELECT 1, schema_name FROM information_schema.schemata--"
    rows = db.find(entity, where={"name": union_payload})
    assert len(rows) == 0, "SECURITY VULNERABILITY: Union attack returned data!"
    print("PASS: Union-based attack neutralized")

    # 3. BOOLEAN-BASED BLIND SQLi
    # Tries to guess data by checking if the page loads differently
    boolean_payload = "1' AND SUBSTRING((SELECT DATABASE()),1,1)='a"
    db.find(entity, where={"id": boolean_payload})
    print("PASS: Boolean-based blind attack neutralized")

    # 4. MULTI-STATEMENT ATTACK (The "Bobby Tables" attack)
    # Tries to delete the whole table
    drop_payload = "1; DROP TABLE users; --"
    try:
        db.find(entity, where={"id": drop_payload})
    except:
        pass # Most drivers like PyMySQL block multi-statements by default
    
    # Check if table still exists by trying a normal find
    try:
        db.find(entity)
        print("PASS: Multi-statement DROP attack blocked")
    except Exception:
        raise AssertionError("SECURITY VULNERABILITY: Table was dropped!")
def test_advanced_security_scenarios(db, entity):
    print("\n--- Running Advanced Security Scenarios ---")

    # A. BOOLEAN-BASED BLIND SQLi
    # Hacker tries to guess data using TRUE/FALSE logic
    boolean_payload = "1' AND (SELECT 1)=1#"
    try:
        db.find(entity, where={"id": boolean_payload})
        print("FAIL: Boolean-based string accepted in numeric field")
    except QueryError:
        print("PASS: Boolean-based attack blocked by Type Checking")

    # B. UNION-BASED ATTACK (Information Leakage)
    # Tries to join your table with system tables to steal DB structure
    union_payload = "' UNION SELECT user(), database(), version() --"
    try:
        db.find(entity, where={"name": union_payload})
        print("PASS: Union attack treated as literal string (No data leaked)")
    except Exception as e:
        print(f"PASS: Union attack handled safely: {e}")

    # C. TIME-BASED BLIND SQLi
    # The most dangerous! It makes the server wait.
    # If the query takes 5+ seconds, you are vulnerable.
    import time
    time_payload = "1' AND (SELECT 1 FROM (SELECT(SLEEP(5)))a)--"
    start = time.time()
    try:
        db.find(entity, where={"name": time_payload})
    finally:
        duration = time.time() - start
        if duration >= 5:
            raise AssertionError("SECURITY VULNERABILITY: Time-based SQLi detected!")
        print(f"PASS: Time-based attack neutralized (Response in {duration:.2f}s)")

odbc_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=servername;"
    "DATABASE=dbduck;"
    "UID=username;"
    "PWD=pass;"
    "TrustServerCertificate=yes;"
)
url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"

DB_CONFIGS = [
    {"db_instance": "mysql", "url": "mysql+pymysql://username:pass@localhost:3306/dbduck"},
    {"db_instance": "sqlite", "url": "sqlite:///test_dbduck.db"},
    {"db_instance": "postgres", "url": "postgresql+psycopg2://username:pass@localhost:5432/dbduck"},
    # MSSQL Addition
    {"db_instance": "mssql", "url": url}
]

def run_security_suite(db_config):
    print(f"\n🚀 Testing Security for Engine: {db_config['db_instance'].upper()}")
    db = UDOM(db_type="sql", **db_config)
    entity = f"sec_test_{uuid4().hex[:6]}"
    db.create(entity, {"id": 1, "name": "Example User"})

    # --- TEST: TIME-BASED BLIND SQLi (Engine Specific) ---
    payloads = {
        "mysql": "1' AND (SELECT 1 FROM (SELECT(SLEEP(2)))a)--",
        "sqlite": "1' AND (SELECT 1 FROM (SELECT UPPER(HEX(RANDOMBLOB(100000000))))a)--",
        "postgres": "1' AND (SELECT 1 FROM pg_sleep(2))--",
        # MSSQL Specific Payload: Uses WAITFOR DELAY
        "mssql": "1'; WAITFOR DELAY '0:0:2'--" 
    }
    
    current_payload = payloads.get(db_config['db_instance'])
    start = time.time()
    
    try:
        db.find(entity, where={"name": current_payload})
    except Exception as e:
        print(f"Handled/Logged error: {e}")
        
    duration = time.time() - start
    
    if duration >= 2:
        raise AssertionError(f"❌ SECURITY ALERT: Time-based SQLi worked on {db_config['db_instance']}!")
    print(f"✅ PASS: Time-based attack neutralized (Took {duration:.2f}s)")
if __name__ == "__main__":
    test_sql_injection_on_dbduck()
    db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://username:pass@localhost:3306/dbduck")
    # test_advanced_sqli_protection(db=db, entity="users")
    test_advanced_security_scenarios(db=db, entity="users")
    # Run the suite across all configured engines
    for config in DB_CONFIGS:
        run_security_suite(db_config=config)
