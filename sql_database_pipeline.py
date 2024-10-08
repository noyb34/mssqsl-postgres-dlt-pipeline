import os

import dlt
import psycopg2
from dlt.sources.sql_database import sql_database


def set_replica_identity(pipeline):
    schema_name = "my_schema_name"
    # Retrieve database connection parameters from environment variables or pipeline config
    dbname = os.getenv("DESTINATION_POSTGRES_DATABASE") or "datalake_house"
    user = os.getenv("DESTINATION_POSTGRES_USER") or "myusername"
    password = os.getenv("DESTINATION_POSTGRES_PASSWORD") or "changeme"
    host = os.getenv("DESTINATION_POSTGRES_HOST", "myapp")
    port = os.getenv("DESTINATION_POSTGRES_PORT", "50432")

    if not all([dbname, user, password]):
        raise ValueError("Database connection parameters are not properly set.")

    conn_params = {
        "dbname": dbname,
        "user": user,
        "password": password,
        "host": host,
        "port": port,
    }

    # Connect to the database
    try:
        conn = psycopg2.connect(**conn_params)
    except Exception as e:
        raise Exception(f"Failed to connect to the database: {e}")

    for table_name in pipeline.default_schema.tables.keys():
        if table_name.startswith("_dlt_"):
            continue  # Skip internal DLT tables
        table_name = table_name.lower()
        # Use the dlt-generated key
        index_name = f"{table_name}_dlt_id_key"
        # Check if the index exists
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 1 FROM pg_indexes
                WHERE schemaname = '{schema_name}'
                AND tablename = '{table_name}'
                AND indexname = '{index_name}';
            """)
            index_exists = cursor.fetchone() is not None

        # If the index doesn't exist, create it
        if not index_exists:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        CREATE UNIQUE INDEX {index_name}
                        ON "{schema_name}"."{table_name}" (_dlt_id);
                    """)
                    conn.commit()
                print(
                    f"Created index {index_name} for table {schema_name}.{table_name}"
                )
            except Exception as e:
                conn.rollback()
                print(f"Failed to create index for table {table_name}: {e}")
                continue
        # Set the replica identity
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    ALTER TABLE "{schema_name}"."{table_name}"
                    REPLICA IDENTITY USING INDEX "{index_name}";
                """)
                conn.commit()
            print(
                f"Set REPLICA IDENTITY USING INDEX {index_name} for table {schema_name}.{table_name}"
            )
        except Exception as e:
            conn.rollback()
            print(f"Failed to set replica identity for table {table_name}: {e}")

    conn.close()


def load_tables():
    # Configure the source to load the specific tables
    source = sql_database().with_resources("table_1", "table_2")

    # Define primary keys for each table
    source.table_1.apply_hints(primary_key="uid")
    source.table_2.apply_hints(primary_key="some_id")

    # Add incremental config to the resources
    source.table_1.apply_hints(incremental=dlt.sources.incremental("updated_at"))
    source.table_2.apply_hints(incremental=dlt.sources.incremental("updated_at"))

    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline_name",
        destination="postgres",
        dataset_name="my_schema_name",
        progress=dlt.progress.alive_progress(
            single_bar=True,
            bar="blocks",
            spinner="waves3",
            theme="smooth",
            refresh_secs=0.5,
        ),
    )

    # Initialize the pipeline to create the schema
    pipeline.run(source, write_disposition="replace")

    # Set replica identity for the tables
    set_replica_identity(pipeline)

    print("Starting extract and data load with merge write disposition...")
    info = pipeline.run(source, write_disposition="merge")
    print(info)

    print("Data load complete and replica identity set.")


if __name__ == "__main__":
    load_tables()
