import os
import asyncio
import requests
import json
import polars as pl
from bs4 import BeautifulSoup

base_url = 'https://www.fortiguard.com'

def build_url(level, page):
    return f'{base_url}/encyclopedia?type=ips&risk={level}&page={page}'

def scrape_page(level, page, data, skipped_pages):
    url = build_url(level, page)
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        title_element = soup.find('div', class_='title')
        link_element = title_element.find('a') if title_element else None
        if link_element:
            title = title_element.text
            link = base_url + link_element['href']
            data.extend([(title, link)])
        else:
            print(f"Skipping level {level}, page {page} due to missing link.")
            skipped_pages.append(page)
    except Exception as e:
        print(f"Exception while scraping level {level}, page {page}: {str(e)}")
        skipped_pages.append(page)

async def scrape_data(level, max_pages):
    data = []
    skipped_pages = []

    for i in range(1, max_pages + 1):
        scrape_page(level, i, data, skipped_pages)

    df = pl.DataFrame(data, schema=['title', 'link'])
    df.write_csv(f'datasets/forti_lists_{level}.csv')

    if skipped_pages:
        with open(f'datasets/skipped.json', 'w') as skipped_file:
            json.dump(skipped_pages, skipped_file)

async def main():
    levels = [1, 2, 3, 4, 5]
    max_pages = [10, 10, 10, 10, 10]
    
    if not os.path.exists('datasets'):
        os.makedirs('datasets')

    tasks = []
    for level, max_page in zip(levels, max_pages):
        tasks.append(scrape_data(level, max_page))

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
