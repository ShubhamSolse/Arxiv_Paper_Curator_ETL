import json
import psycopg2
from psycopg2.extras import Json
from airflow.sdk import dag, task
from transform import transformed_asset

@dag(
    schedule=[transformed_asset],
    tags=['arxiv', 'load']
)
def arxiv_paper_loader():
    
    @task
    def load_to_postgres():
        with open(transformed_asset.uri, 'r') as f:
            data = json.load(f)
        
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS research_papers (
                id SERIAL PRIMARY KEY,
                search_query VARCHAR(255),
                paper_id VARCHAR(255),
                title TEXT,
                updated TIMESTAMP,
                published TIMESTAMP,
                summary TEXT,
                authors JSONB,
                categories JSONB,
                pdf_link TEXT,
                pdf_content JSONB
            )
        """)
        
        for paper in data['papers']:
            cur.execute("""
                INSERT INTO research_papers 
                (search_query, paper_id, title, updated, published, summary, authors, categories, pdf_link, pdf_content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['search_query'],
                paper['id'],
                paper['title'],
                paper['updated'],
                paper['published'],
                paper['summary'],
                Json(paper['authors']),
                Json(paper['categories']),
                paper['pdf_link'],
                Json(paper['pdf_content'])
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        return f"Loaded {len(data['papers'])} papers"
    
    load_to_postgres()

arxiv_paper_loader()
