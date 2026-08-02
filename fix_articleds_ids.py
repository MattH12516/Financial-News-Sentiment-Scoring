import pipeline
conn = pipeline.get_db_connection()
conn.execute("DELETE FROM ticker_mentions WHERE article_id NOT IN (SELECT id FROM articles)")
conn.commit()
print("orphans left:", conn.execute(
    "SELECT COUNT(*) FROM ticker_mentions WHERE article_id NOT IN (SELECT id FROM articles)"
).fetchone()[0])