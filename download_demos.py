import logging
import time
import re
import os
from curl_cffi import requests
from lxml import etree
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class HltvMatchScraper:
    def __init__(self, stars=3):
        self.base_url = "https://www.hltv.org"
        self.results_url = "https://www.hltv.org/results"
        self.stars = stars
        self.download_dir = "demos"
        os.makedirs(self.download_dir, exist_ok=True)

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Referer": "https://www.hltv.org/"
        }
        self.empty_count = 0
        self.max_empty = 2

    def get_match_urls(self):
        match_urls = []
        offset = 0

        while True:
            url = f"{self.results_url}?offset={offset}&stars={self.stars}"
            logging.info(f"Fetching page: {url}")

            try:
                resp = requests.get(
                    url,
                    headers=self.headers,
                    impersonate="chrome124",
                    timeout=15
                )
                if 400 <= resp.status_code < 500:
                    logging.error(f"Client error {resp.status_code}, stop scrape")
                    break

            except Exception as e:
                logging.error(f"Request error: {e}")
                time.sleep(2)
                continue

            tree = etree.HTML(resp.text)
            elements = tree.xpath(
                '//div[@class="results-all"]//div[contains(@class,"result-con")]/a[@class="a-reset"]/@href')
            page_num = len(elements)

            if page_num == 0:
                self.empty_count += 1
                logging.warning(f"Empty page count: {self.empty_count}/{self.max_empty}")
                if self.empty_count >= self.max_empty:
                    logging.info("No more results, exit loop")
                    break
                time.sleep(2)
                continue

            self.empty_count = 0
            for link in elements:
                full = f"{self.base_url}{link}"
                match_urls.append(full)
                logging.info(f"Found: {full}")
                self.download_demo(full)

            logging.info(f"Page offset {offset} total: {page_num}")
            offset += 100
            time.sleep(1.2)

        logging.info(f"Total matches: {len(match_urls)}")
        return match_urls

    def download_demo(self, match_url):
        try:
            resp = requests.get(
                match_url,
                headers=self.headers,
                impersonate="chrome124",
                timeout=15
            )
            tree = etree.HTML(resp.text)
            demo_path = tree.xpath('//a[@data-demo-link]/@data-demo-link')
            if not demo_path:
                logging.warning(f"No demo found: {match_url}")
                return

            demo_url = f"{self.base_url}{demo_path[0]}"
            demo_resp = requests.get(
                demo_url,
                headers=self.headers,
                impersonate="chrome124",
                timeout=120,
                allow_redirects=True,
                stream=True
            )

            if demo_resp.status_code not in (200, 201):
                logging.warning(f"Download failed {demo_resp.status_code}: {demo_url}")
                return

            filename = None
            if "Content-Disposition" in demo_resp.headers:
                disp = demo_resp.headers["Content-Disposition"]
                fname = re.findall(r'filename[^;=\n]*=((["\']).*?\2|[^;\n]*)', disp)
                if fname:
                    filename = fname[0][0].replace('"', '').replace("'", "")

            if not filename:
                filename = demo_resp.url.split("/")[-1]

            total_size = int(demo_resp.headers.get("content-length", 0))
            file_path = os.path.join(self.download_dir, filename)

            with open(file_path, 'wb') as f, tqdm(total=total_size, unit='B', unit_scale=True,
                                                  unit_divisor=1024) as bar:
                for chunk in demo_resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))

            logging.info(f"Downloaded: {file_path}")
            time.sleep(5)

        except Exception as e:
            logging.error(f"Demo error: {e}")


if __name__ == "__main__":
    scraper = HltvMatchScraper(stars=5)
    all_matches = scraper.get_match_urls()
