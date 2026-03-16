import xml.etree.ElementTree as ET
import json
import os
import re
import requests
from io import BytesIO
import pdfplumber
from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset
from extract import arxiv_asset

transformed_asset = Asset(uri='/opt/airflow/logs/data/data_transform.json')

@dag(
    schedule= "@daily",
    tags=['arxiv', 'transformation']
)

def arxiv_paper_transformer():

    @task(outlets=[transformed_asset])
    def transform_data():
        with open(arxiv_asset.uri, 'r', encoding='utf-8') as f:
            data = f.read()
        
        xml_feeds = re.findall(r'<\?xml.*?</feed>', data, re.DOTALL)
        ns = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}
        
        all_papers = []
        pdf_count = 0
        
        for xml_content in xml_feeds:
            root = ET.fromstring(xml_content)
            
            for entry in root.findall('atom:entry', ns):
                pdf_link = next((link.get('href') for link in entry.findall('atom:link', ns) if link.get('type') == 'application/pdf'), None)
                
                pdf_paragraphs = None
                if pdf_link:
                    try:
                        print(f"Downloading PDF {pdf_count + 1}: {pdf_link}")
                        response = requests.get(pdf_link, timeout=30)
                        pdf_file = BytesIO(response.content)
                        with pdfplumber.open(pdf_file) as pdf:
                            text_blocks = []
                            for page in pdf.pages[:3]:
                                text = page.extract_text()
                                if text:
                                    blocks = [b.strip() for b in re.split(r'\n{2,}', text) if b.strip()]
                                    text_blocks.extend(blocks)
                            
                            cleaned = []
                            for para in text_blocks:
                                para = re.sub(r'\d{4}\s*voN\s*\d+\s*\].*?\[.*?viXra', '', para)
                                para = re.sub(r'\s+', ' ', para)
                                para = para.replace('\x00', '').replace('\u0000', '')
                                if len(para) > 100:
                                    cleaned.append(para.strip())
                            
                            pdf_paragraphs = cleaned[:10]
                        print(f"Extracted {len(pdf_paragraphs)} paragraphs")
                        pdf_count += 1
                        pdf_file.close()
                        del pdf_file, response
                    except Exception as e:
                        print(f"Error extracting PDF: {e}")
                
                paper = {
                    'id': entry.find('atom:id', ns).text,
                    'title': entry.find('atom:title', ns).text,
                    'updated': entry.find('atom:updated', ns).text,
                    'published': entry.find('atom:published', ns).text,
                    'summary': entry.find('atom:summary', ns).text,
                    'authors': [author.find('atom:name', ns).text for author in entry.findall('atom:author', ns)],
                    'categories': [cat.get('term') for cat in entry.findall('atom:category', ns)],
                    'pdf_link': pdf_link,
                    'pdf_content': pdf_paragraphs
                }
                all_papers.append(paper)
        
        result = {'search_query': 'all', 'papers': all_papers, 'total_count': len(all_papers)}

        os.makedirs(os.path.dirname(transformed_asset.uri), exist_ok=True)
        with open(transformed_asset.uri, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        return transformed_asset.uri
    
    transform_data()

arxiv_paper_transformer()


            
