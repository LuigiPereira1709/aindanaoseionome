import sqlite3

conn = sqlite3.connect('db/load_db/company_metrics.db')
with open('dump.sql', 'w') as f:
    for linha in conn.iterdump():
        f.write('%s\n' % linha)

conn.close()