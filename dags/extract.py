from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset
from pendulum import datetime
import urllib.request
import urllib.parse
import time
import os
import logging

logging.basicConfig(level=logging.INFO)

arxiv_asset = Asset(uri='/opt/airflow/logs/data/data_extract.xml')

@dag(
    schedule="@daily",
    tags=['arxiv', 'extraction']
)
def arxiv_paper_extractor():
    
    @task(outlets=[arxiv_asset])
    def extract_and_save():
        count_file = '/opt/airflow/logs/data/count.txt'
        os.makedirs(os.path.dirname(count_file), exist_ok=True)
        
        if os.path.exists(count_file):
            with open(count_file, 'r') as f:
                start_from = int(f.read().strip())
        else:
            start_from = 0
        
        logging.info(f"Starting from count: {start_from}")
        
        results_per_page = 10
        total_results = 50
        all_papers = []
        
        
        logging.info(f"Fetching papers for: {'all'}")
        start = start_from
        end = start_from + total_results
        
        while start < end:
            url = f'http://export.arxiv.org/api/query?search_query=all&start={str(start)}&max_results={str(results_per_page)}'
            logging.info(f"Fetching from {start} to {start + results_per_page}")
            
            data = urllib.request.urlopen(url)
            response = data.read().decode('utf-8')
            all_papers.append(('all', start, response))
            
            start += results_per_page
            time.sleep(3)
        
        os.makedirs(os.path.dirname(arxiv_asset.uri), exist_ok=True)
        with open(arxiv_asset.uri, 'w', encoding='utf-8') as f:
            for query, start_pos, content in all_papers:
                f.write(f"\n{'='*80}\nQuery: {query} | Start: {start_pos}\n{'='*80}\n")
                f.write(content)
        
        new_count = start_from + total_results
        with open(count_file, 'w') as f:
            f.write(str(new_count))
        
        logging.info(f"Extracted {len(all_papers)} batches. Updated count to {new_count}")
        return arxiv_asset.uri
    
    extract_and_save()

arxiv_paper_extractor()
