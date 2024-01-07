import sqlite3
import pandas as pd
from pprint import pprint

# Establish the connection with the database
conn = sqlite3.connect('db/source_data/software_companies.db')
c = conn.cursor()

# SQL query to retrieve general information
query1 = '''
SELECT 
    M.Rank AS company_id,
    M.Name,
    M.Symbol,
    M.Country
FROM 
    market_cap M
INNER JOIN 
    employees E ON M.name = E.name
INNER JOIN
    operating_margin O ON M.name = O.name
INNER JOIN
    P_E P ON M.name = P.name
'''

# SQL query to retrieve detailed financial information
query2 = '''
SELECT 
    M.Rank AS company_id,
    M.marketcap,
    E.employees_count,
    M."price (USD)" AS price_usd,
    O.operating_margin_ttm,
    P.pe_ratio_ttm
FROM 
    market_cap M
INNER JOIN 
    employees E ON M.name = E.name
INNER JOIN
    operating_margin O ON E.name = O.name
INNER JOIN
    P_E P ON E.name = P.name
'''

# Read the results of the queries into pandas DataFrames
df_general_information = pd.read_sql_query(query1, conn)
df_financial_information = pd.read_sql_query(query2, conn)

# Merge the DataFrames on 'company_id'
df_merged = pd.merge(df_general_information, df_financial_information, on='company_id')

# Remove rows with zero values and NaNs
df_merged = df_merged.loc[(df_merged != 0).all(axis=1)].dropna()

# Get the common columns between df_merged and df_financial_information
common_columns = df_merged.columns.intersection(df_financial_information.columns)

# Select only the common columns from df_merged
df_financial_information = df_merged[common_columns]

# Filter the DataFrames to keep only the rows with common 'company_id'
commons_company_id = df_general_information['company_id'].isin(df_financial_information['company_id'])
df_general_information = df_general_information.loc[commons_company_id]
df_financial_information = df_financial_information.loc[commons_company_id]

# Save the DataFrames to CSV files
df_general_information.to_csv('data/LOAD/general_information.csv', index=False)
df_financial_information.to_csv('data/LOAD/detailed_financial_info.csv', index=False)

# Commit and close the connection
conn.commit()
conn.close()
