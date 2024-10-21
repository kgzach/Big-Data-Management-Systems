import sqlite3
import pandas as pd


def loadDataFromDb(db_path):
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql('SELECT * FROM vehicle_data', conn)
    finally:
        conn.close()
    return df
