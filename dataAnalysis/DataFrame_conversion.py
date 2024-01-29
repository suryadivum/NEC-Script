from pathlib import Path
from tableauhyperapi import HyperProcess, Connection, Telemetry
import pandas as pd
import os


def search_hyper_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.hyper'):
                return Path(str(os.path.join(root, file)))
    return None


def search_xlsx_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.xlsx') or file.lower().endswith('.xls'):
                return Path(str(os.path.join(root, file)))
    return None


def search_csv_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.csv'):
                return Path(str(os.path.join(root, file)))
    return None


def convert_hyper_to_dataframe(hyper_file):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
            table_names = connection.catalog.get_table_names("Extract")
            if table_names:
                table_name = table_names[0]
                table_definition = connection.catalog.get_table_definition(table_name)
                column_names = [column.name for column in table_definition.columns]
                query = f"SELECT * FROM {table_name}"
                result = connection.execute_query(query)
                my_dict = {key: value_list for key, value_list in zip(column_names, zip(*result))}
                result.close()
                data = pd.DataFrame(my_dict)
                return data
            else:
                print(f'No tables found in the hyper file: "{hyper_file}"')
                return None


def convert_xlsx_to_dataframe(xlsx_path):
    try:
        return pd.DataFrame(pd.read_excel(xlsx_path))
    except Exception as e:
        print(str(e))
        return None


def convert_csv_to_dataframe(csv_path):
    try:
        return pd.DataFrame(pd.read_csv(csv_path))
    except Exception as e:
        print(str(e))
        return None


data_source_path = './ExtractFiles'
df = None

hyper_file_path = search_hyper_files(data_source_path)
if hyper_file_path is not None:
    df = convert_hyper_to_dataframe(hyper_file_path)
else:
    xlsx_file_path = search_xlsx_files(data_source_path)
    if xlsx_file_path is not None:
        df = convert_xlsx_to_dataframe(xlsx_file_path)
    else:
        csv_file_path = search_csv_files(data_source_path)
        if csv_file_path is not None:
            df = convert_csv_to_dataframe(csv_file_path)

if df is not None:
    print(df.head())
