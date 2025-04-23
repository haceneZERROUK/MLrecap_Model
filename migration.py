import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

# Charger les variables d’environnement
load_dotenv()

AZURE_SERVER = os.getenv("AZURE_SERVER")
AZURE_DATABASE = os.getenv("AZURE_DATABASE")
AZURE_USERNAME = os.getenv("AZURE_USERNAME")
AZURE_PASSWORD = os.getenv("AZURE_PASSWORD")

# Connexion à Azure SQL
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={AZURE_SERVER};"
    f"DATABASE={AZURE_DATABASE};"
    f"UID={AZURE_USERNAME};"
    f"PWD={AZURE_PASSWORD};"
    f"TrustServerCertificate=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Charger le fichier JSON
df = pd.read_json("DATASET_FINAL.json")

# Supprimer la table si elle existe déjà
cursor.execute("IF OBJECT_ID('movies', 'U') IS NOT NULL DROP TABLE movies")

# Créer la table manuellement avec des types adaptés
cursor.execute("""
CREATE TABLE movies (
    fr_title NVARCHAR(255),
    released_year INT,
    directors NVARCHAR(255),
    writer NVARCHAR(255),
    distribution NVARCHAR(255),
    country NVARCHAR(100),
    budget FLOAT,
    category NVARCHAR(100),
    released_date NVARCHAR(50),
    classification NVARCHAR(100),
    duration NVARCHAR(50),
    weekly_entrances INT,
    duration_minutes INT,
    actor_1 NVARCHAR(255),
    actor_2 NVARCHAR(255),
    actor_3 NVARCHAR(255)
)
""")

# Insérer les données dans la table
insert_query = """
INSERT INTO movies (
    fr_title, released_year, directors, writer, distribution, country, budget,
    category, released_date, classification, duration, weekly_entrances,
    duration_minutes, actor_1, actor_2, actor_3
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for _, row in df.iterrows():
    cursor.execute(insert_query, (
        row['fr_title'], row['released_year'], row['directors'], row['writer'],
        row['distribution'], row['country'], row['budget'], row['category'],
        row['released_date'], row['classification'], row['duration'],
        row['weekly_entrances'], row['duration_minutes'],
        row['actor_1'], row['actor_2'], row['actor_3']
    ))

conn.commit()
conn.close()
print("✅ Données insérées avec succès dans Azure SQL.")
