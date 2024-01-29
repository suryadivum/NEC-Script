from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, escape_string_literal, TableName, \
    HyperException
import zipfile
import os


def search_tde_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.tde'):
                return Path(str(os.path.join(root, file)))
    return None


def extract_hyper_from_twbx(twbx_file_path, output_directory):
    with zipfile.ZipFile(twbx_file_path, 'r') as zip_ref:
        zip_ref.extractall(output_directory)


def convert_tde_to_hyper(tde_path):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        hyper_path = tde_path.with_name(tde_path.stem + '.hyper')
        with Connection(endpoint=hyper.endpoint, database=hyper_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            try:
                existing_tables = connection.execute_list_query(f"""
                    SELECT "SCHEMAS_NAME", "TABLES_NAME"
                    FROM external({escape_string_literal(str(tde_path))}, format => 'tde', "table" => 'SYS.TABLES')
                    JOIN external({escape_string_literal(str(tde_path))}, format => 'tde', "table" => 'SYS.SCHEMAS')
                    ON "TABLES_PARENT"="SCHEMAS_ID"
                    WHERE "SCHEMAS_NAME" <> 'SYS' AND "TABLES_NAME"<>'$TableauMetadata' AND "TABLES_ACTIVE" AND "SCHEMAS_ACTIVE"
                """)
                for schema, table in existing_tables:
                    # Create the destination table in the Hyper database
                    connection.catalog.create_schema_if_not_exists(schema)
                    connection.execute_command(f"""
                        CREATE TABLE {TableName(schema, table)} AS
                        SELECT * FROM external({escape_string_literal(str(tde_path))}, format => 'tde', "table" => {escape_string_literal(f"{schema}.{table}")})""")
            except HyperException:
                os.unlink(hyper_path)
                print(f"FAILED conversion {tde_path} -> {hyper_path}")


twbx_file = Path('./Climate Change.twbx')
output_path = Path('./ExtractFiles')

extract_hyper_from_twbx(twbx_file, output_path)

tde_file = search_tde_files(output_path)
if tde_file:
    convert_tde_to_hyper(tde_file)
