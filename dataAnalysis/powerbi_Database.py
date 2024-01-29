import os
import glob
from sqlalchemy import create_engine
import pandas as pd
from Database import Database
# Replace these variables with your PostgresSQL connection details
db_user = Database.user
db_password = Database.password
db_host = Database.host
db_port = Database.port
db_name = Database.database

# Construct the PostgresSQL connection string
connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create a SQLAlchemy engine
engine = create_engine(connection_string)

# Print a message to confirm if the engine is successfully created
print("Engine created successfully!")

# Get the current working directory
current_directory = os.getcwd()

# Use glob to get a list of all CSV files in the current directory
csv_files = glob.glob(os.path.join(current_directory, '*.csv'))

# Iterate through CSV files and insert data into the database
for csv_file in csv_files:
    # Extract the table name from the CSV file name (excluding the extension)
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    try:
        df = pd.read_csv(csv_file.strip())
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(table_name, " inserted sucessfully!")
    except Exception as e:
        pass

# Dispose of the engine
engine.dispose()
