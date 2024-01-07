from sqlite3 import connect, Error
from pandas import read_csv
import os
from time import sleep

# Folder containing CSV files
source_data = 'data/source_data/'

for file_name in os.listdir(source_data):
    if file_name.endswith('.csv'):
        try:
            sleep(3.5)
            # Clear the console screen
            os.system('cls' if os.name == 'nt' else 'clear')

            # Build the complete file path
            file_path = os.path.join(source_data, file_name)

            # Read the CSV file into a pandas DataFrame
            df = read_csv(file_path)

            print(file_name)

            name_db = 'software_companies'

            # Establish a connection with the SQLite database
            conn = connect(f'db/source_data/{name_db}.db')

            # Ask user for the table name
            name_table = input('Name of the table: ')

            # Store the DataFrame into the SQL database
            df.to_sql(name_table, con=conn, index=False, if_exists='replace', method='multi', chunksize=100)

            # Display success message
            print(f"Data from {file_name} successfully imported into table {name_table} in database {name_db}.\n")
        except Error as e:
            # Print error message
            print(f"Error: {e}\n")
        finally:
            # Always close the connection, even in case of error
            pass
            if conn:
                conn.commit()
                conn.close()

