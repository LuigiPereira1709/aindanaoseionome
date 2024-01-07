import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('db/load_db/company_metrics.db')
c = conn.cursor()

# Define the schema for the financial_metrics_fact table
drop_table_fact = 'DROP TABLE IF EXISTS financial_metrics_fact;'
create_table_fact = '''
CREATE TABLE financial_metrics_fact (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    company_id INTEGER NOT NULL,
    market_cap INTEGER,
    employees_count INTEGER,
    price_usd FLOAT,
    operating_margin_ttm FLOAT,
    pe_ratio_ttm FLOAT,
    FOREIGN KEY (company_id) REFERENCES company_dimension(company_id)
);
'''

# Define the schema for the company_dimension table
drop_table_dimension = 'DROP TABLE IF EXISTS company_dimension;'
create_table_dimension = '''
CREATE TABLE company_dimension (
    company_id INTEGER PRIMARY KEY NOT NULL,
    name VARCHAR(255),
    symbol CHAR,
    country VARCHAR(255)
);
'''

# Execute SQL commands to drop and create the tables
c.execute(drop_table_fact)
c.execute(create_table_fact)

c.execute(drop_table_dimension)
c.execute(create_table_dimension)

# Commit the changes and close the connection to the database
conn.commit()
conn.close()