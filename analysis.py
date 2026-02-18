import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from config import Config

# Daten laden
conn = sqlite3.connect(Config.DB_FILE)

# Posts nach Datum gruppieren
df = pd.read_sql_query("""
    SELECT DATE(timestamp) as date, COUNT(*) as post_count
    FROM posts
    GROUP BY DATE(timestamp)
    ORDER BY date
""", conn)

# Visualisierung
plt.figure(figsize=(12, 6))
plt.plot(pd.to_datetime(df['date']), df['post_count'])
plt.title('Posts pro Tag')
plt.xlabel('Datum')
plt.ylabel('Anzahl Posts')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('posts_timeline.png')

# Top Hashtags
hashtags_df = pd.read_sql_query("""
    SELECT hashtags FROM posts WHERE hashtags != '[]'
""", conn)

conn.close()
print("✅ Analyse abgeschlossen!")