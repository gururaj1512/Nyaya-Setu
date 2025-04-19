import os
import requests
from bs4 import BeautifulSoup
import hashlib
from urllib.parse import urljoin, urlparse
import time
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pdf_scraper.log"),
        logging.StreamHandler()
    ]
)

class PDFScraper:
    def __init__(self, output_dir="downloaded_pdfs"):
        self.output_dir = output_dir
        self.downloaded_urls = set()
        self.downloaded_hashes = set()

        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Load previously downloaded URLs from file if it exists
        self.history_file = os.path.join(output_dir, "download_history.txt")
        if os.path.exists(self.history_file):
            with open(self.history_file, "r") as f:
                self.downloaded_urls = set(line.strip() for line in f.readlines())
            logging.info(f"Loaded {len(self.downloaded_urls)} previously downloaded URLs")

    def save_url_to_history(self, url):
        with open(self.history_file, "a") as f:
            f.write(url + "\n")
    
    def download_pdf(self, url, filename=None):
        if url in self.downloaded_urls:
            logging.info(f"PDF already downloaded: {url}")
            return False

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            }
            response = requests.get(url, headers=headers, stream=True, timeout=30)

            if response.status_code != 200:
                logging.error(f"Failed to download PDF: {url} (Status code: {response.status_code})")
                return False

            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/pdf' not in content_type and not url.lower().endswith('.pdf'):
                logging.warning(f"URL may not be a PDF: {url} (Content-Type: {content_type})")

            if not filename:
                if "Content-Disposition" in response.headers:
                    import re
                    matches = re.findall('filename="(.+)"', response.headers["Content-Disposition"])
                    if matches:
                        filename = matches[0]

                if not filename:
                    parsed_url = urlparse(url)
                    filename = os.path.basename(parsed_url.path)

                    if not filename or not filename.lower().endswith('.pdf'):
                        url_hash = hashlib.md5(url.encode()).hexdigest()
                        filename = f"{url_hash}.pdf"

            if not filename.lower().endswith('.pdf'):
                filename += '.pdf'

            filepath = os.path.join(self.output_dir, filename)
            
            # Ensure we don't overwrite existing files with different content
            counter = 1
            original_filename = filename.replace('.pdf', '')
            while os.path.exists(filepath):
                filename = f"{original_filename}_{counter}.pdf"
                filepath = os.path.join(self.output_dir, filename)
                counter += 1
            
            # Download and save the file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Calculate file hash to avoid duplicate content
            file_hash = self._calculate_file_hash(filepath)
            if file_hash in self.downloaded_hashes:
                logging.info(f"Duplicate content detected, removing: {filepath}")
                os.remove(filepath)
                return False

            self.downloaded_hashes.add(file_hash)
            self.downloaded_urls.add(url)
            self.save_url_to_history(url)

            file_size = os.path.getsize(filepath) / 1024  # Size in KB
            logging.info(f"Downloaded PDF: {url} -> {filename} ({file_size:.2f} KB)")
            return True

        except Exception as e:
            logging.error(f"Error downloading {url}: {str(e)}")
            return False

    def _calculate_file_hash(self, filepath):
        """Calculate MD5 hash of a file"""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def extract_pdf_links(self, html_content, base_url):
        soup = BeautifulSoup(html_content, 'html.parser')
        pdf_links = []
        
        # Look for the specific pattern in the DOJ website
        pdf_containers = soup.select("span.pdf-downloads")
        
        for container in pdf_containers:
            # Find all PDF links in the container
            links = container.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and ('.pdf' in href.lower() or 'download' in link.get('class', [])):
                    # Resolve relative URLs
                    full_url = urljoin(base_url, href)
                    pdf_links.append(full_url)
        
        return pdf_links
    
    def scrape_page(self, url):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            }
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logging.error(f"Failed to fetch page: {url} (Status code: {response.status_code})")
                return 0

            pdf_links = self.extract_pdf_links(response.text, url)
            logging.info(f"Found {len(pdf_links)} PDF links on {url}")

            # Download each PDF
            downloaded_count = 0
            for pdf_url in pdf_links:
                if self.download_pdf(pdf_url):
                    downloaded_count += 1
                # Add delay to avoid overwhelming the server
                time.sleep(1)
            
            return downloaded_count
            
        except Exception as e:
            logging.error(f"Error scraping {url}: {str(e)}")
            return 0
    
    def scrape_multiple_pages(self, urls):
        total_downloaded = 0
        for url in urls:
            logging.info(f"Scraping {url}")
            downloaded = self.scrape_page(url)
            total_downloaded += downloaded
            logging.info(f"Downloaded {downloaded} PDFs from {url}")
            # Add delay between pages
            time.sleep(2)
        
        logging.info(f"Finished scraping. Total PDFs downloaded: {total_downloaded}")
        return total_downloaded
    
class LoadUrls:
    def __init__(self, file_path):
        self.file_path = file_path
    
    def load_urls_from_file(self):
        urls = []
        with open(self.file_path, 'r') as file:
            for line in file:
                url = line.strip()
                if url:
                    urls.append(url)
        return urls


# Example usage
if __name__ == "__main__":
    file_path = './url-extractor/discovered_urls.txt'
    loader = LoadUrls(file_path)
    url_list = loader.load_urls_from_file()
    doj_urls = url_list
    
    scraper = PDFScraper(output_dir="doj_pdfs")
    scraper.scrape_multiple_pages(doj_urls)
