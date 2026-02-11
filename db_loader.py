import pyodbc
import pandas as pd

def load_amazon_sales():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=DESKTOP-AM7FUKM;"
        "DATABASE=Restart_Practice;"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    query = "SELECT * FROM [dbo].['Amazon_Sale_Report']"
    df = pd.read_sql(query, conn) 
    conn.close()
    return df

if __name__ == "__main__":
    df = load_amazon_sales()
    print("Connection successful ✅")
    print(df.head())          # shows first 5 rows
    print("Total rows:", len(df))