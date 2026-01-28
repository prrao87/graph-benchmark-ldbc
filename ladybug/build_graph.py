from pathlib import Path

import real_ladybug as lb


def setup_db(db_name: str, overwrite: bool = True) -> lb.Connection:
    """
    Create a new Kuzu database and a graph schema based on DDL commands.
    """
    if overwrite:
        Path(db_name).unlink(missing_ok=True)
    db = lb.Database(db_name)
    conn = lb.Connection(db)

    with open("./etl/schema.cypher", "r") as f:
        schema_ddl = f.read()
    assert schema_ddl.startswith("CREATE")
    conn.execute(schema_ddl)
    print("Schema created successfully.")
    return conn


def ingest_data(conn: lb.Connection, data_path: str):
    """
    Ingest data from the given path into the existing database.
    """
    with open("./etl/copy.cypher", "r") as f:
        copy_ddl = f.read()
    assert copy_ddl.startswith("COPY")
    conn.execute(copy_ddl)
    print("Data ingested successfully.")


if __name__ == "__main__":
    DB_NAME = "ldbc_snb_sf1.lbdb"
    DATA_PATH = "../csv"
    conn = setup_db(DB_NAME, overwrite=True)
    ingest_data(conn, DATA_PATH)
