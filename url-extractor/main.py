import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import concurrent.futures
import time
import logging
import os
import dotenv

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class WebCrawler:
    def __init__(self, base_url, output_file='./discovered_urls.txt', max_depth=10, max_pages=500):
        self.base_url = base_url
        self.output_file = output_file
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.visited_urls = set()
        self.all_urls = set()
        
        try:
            self.domain = urlparse(base_url).netloc
            if not self.domain:
                raise ValueError("Invalid URL format")
        except Exception as e:
            logging.error(f"URL parsing error: {e}")
            raise

    def is_valid_url(self, url):
        parsed = urlparse(url)
        return (parsed.netloc == self.domain and 
                parsed.scheme in ['http', 'https'] and
                not any(url.endswith(ext) for ext in ['.pdf', '.jpg', '.png', '.gif', '.css', '.js']))

    def extract_urls(self, url, depth):
        if (url in self.visited_urls or 
            depth > self.max_depth or 
            len(self.visited_urls) >= self.max_pages):
            return set()

        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            self.visited_urls.add(url)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_urls = set()

            for link in soup.find_all('a', href=True):
                full_url = urljoin(url, link['href'])
                
                if self.is_valid_url(full_url) and full_url not in self.all_urls:
                    page_urls.add(full_url)
                    self.all_urls.add(full_url)

            return page_urls

        except requests.RequestException as e:
            logging.warning(f"Request error for {url}: {e}")
            return set()

    def crawl(self):
        current_urls = {self.base_url}
        for depth in range(self.max_depth):
            next_urls = set()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_url = {
                    executor.submit(self.extract_urls, url, depth): url 
                    for url in current_urls
                }
                
                for future in concurrent.futures.as_completed(future_to_url):
                    try:
                        new_urls = future.result()
                        next_urls.update(new_urls)
                    except Exception as exc:
                        logging.error(f'Crawling exception: {exc}')

            current_urls = next_urls
            
            if not current_urls:
                break
            
            time.sleep(1)

        # Use UTF-8 encoding to handle non-ASCII characters
        with open(self.output_file, 'w', encoding='utf-8') as f:
            for url in sorted(self.all_urls):
                f.write(f"{url}\n")

        print(f"Total URLs discovered: {len(self.all_urls)}")
        return self.all_urls

def main():
    base_url = os.getenv('WEBSITE_URL')
    try:
        crawler = WebCrawler(base_url)
        crawler.crawl()
    except Exception as e:
        logging.error(f"Crawling failed: {e}")

if __name__ == "__main__":
    main()