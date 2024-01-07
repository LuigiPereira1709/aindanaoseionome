import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('db/load_db/company_metrics.db')
c = conn.cursor()

# Read CSV files into Pandas DataFrames
df_general_information = pd.read_csv('data/LOAD/general_information.csv')
df_financial_information = pd.read_csv('data/LOAD/detailed_financial_info.csv')

# Map columns from the general information DataFrame
columns_mapping_gn = {
    'Name': 'name',
    'Symbol': 'symbol',
}
df_general_information_mapped = df_general_information[list(columns_mapping_gn.keys())].rename(columns=columns_mapping_gn)

# Rename 'marketcap' column to match the expected column name in the database
df_financial_information = df_financial_information.rename(columns={'marketcap': 'market_cap'})

# Write DataFrames to SQLite tables
df_general_information_mapped.to_sql('company_dimension', conn, if_exists='append', index=False, chunksize=100, method='multi')
df_financial_information.to_sql('financial_metrics_fact', conn, if_exists='append', index=False, chunksize=100, method='multi')

# Commit changes and close the connection
conn.commit()
conn.close()
