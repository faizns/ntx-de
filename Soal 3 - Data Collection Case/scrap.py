import os
import json
import asyncio
import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm
import polars as pl

# Define the base URL and maximum pages for each level
BASE_URL = "https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={i}"
MAX_PAGES = [10, 15, 20, 25, 30]

# Define the output directory
OUTPUT_DIR = "datasets"

# Function to scrape data for a given level and page
async def scrape_data(level, page):
    url = BASE_URL.format(level=level, i=page)
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            titles = [item.text for item in soup.find_all("h4", class_="card-title")]
            links = [item.find("a")["href"] for item in soup.find_all("div", class_="encyclopedia-card")]

            data = pl.DataFrame({"title": titles, "link": links})
            data.write_csv(f"{OUTPUT_DIR}/forti_lists_{level}.csv")
            
            return None  # No exceptions, return None
        except (httpx.HTTPError, httpx.RequestError, httpx.ConnectError, httpx.TimeoutException) as e:
            return (level, page)  # Return a tuple of the level and page for skipped pages

# Function to scrape data for all levels asynchronously
async def scrape_all_levels():
    skipped_pages = []

    for level, max_page in enumerate(MAX_PAGES, start=1):
        tasks = [scrape_data(level, page) for page in range(1, max_page + 1)]
        results = await asyncio.gather(*tasks)

        # Collect skipped pages
        skipped_pages.extend([result for result in results if result is not None])

    # Write skipped pages to JSON
    with open(f"{OUTPUT_DIR}/skipped.json", "w") as json_file:
        json.dump(skipped_pages, json_file)

# Create the output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Run the scraping process
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scrape_all_levels())


# -----------------------
# is not solved 
