import argparse
import logging
import random
import re
import signal
import sys
import threading
from datetime import datetime
from queue import PriorityQueue

import urllib.parse
import urllib.request
import urllib.robotparser

from bs4 import BeautifulSoup

# Priority constants for the crawling queue
PRIORITY_VERY_HIGH = 0
PRIORITY_HIGH = 1
PRIORITY_MEDIUM = 2
PRIORITY_LOW = 3
PRIORITY_VERY_LOW = 4

class Worker(threading.Thread):
    """
    Worker class that represents a single crawling thread.
    It fetches URLs from the queue and processes them.
    """

    def __init__(self, queue, visited_urls, visited_urls_lock, stats):
        """
        Initialize the Worker thread.
        
        :param queue: PriorityQueue containing URLs to crawl
        :param visited_urls: Dictionary to track visited URLs
        :param visited_urls_lock: Dictionary to the lock for thread-safe access to each key-value (domain-urls) pair in visited_urls
        :param stats: Dictionary to store crawling statistics
        """
        threading.Thread.__init__(self, daemon=True)
        self.queue = queue
        self.visited_urls = visited_urls
        self.visited_urls_lock = visited_urls_lock
        self.stats = stats

    def run(self):
        """
        Main loop of the Worker thread.
        Continuously fetches URLs from the queue and crawls them until the queue is empty.
        """
        while True:
            if self.queue.empty():
                break  
            priority, (url, depth) = self.queue.get(timeout=1)
            self.crawl_url(priority, url, depth)
            self.queue.task_done()

    def is_nz_domain(self, url):
        """
        Check if the given URL belongs to a New Zealand domain.
        
        :param url: URL to check
        :return: Boolean indicating whether the URL is from a NZ domain
        """
        is_nz = urllib.parse.urlparse(url).netloc.endswith('.nz') or urllib.parse.urlparse(url).netloc.startswith('nz.')
        return is_nz
     
    def is_blacklisted_url(self, url):
        """
        Check if the given URL is blacklisted based on various criteria.
        
        :param url: URL to check
        :return: Boolean indicating whether the URL is blacklisted
        """
        # Lists of blacklisted prefixes, suffixes, and password-related terms
        PREFIX_BLACKLIST = [
            'http:', 'mailto:', 'tel:', 'javascript:', 'data:', 'ftp:', 'file:'
        ]
        SUFFIX_BLACKLIST = [
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', 
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', 
            '.mp3', '.wav', '.ogg', '.flac', 
            '.mp4', '.avi', '.mov', '.mkv', 
            '.zip', '.tar', '.gz', '.rar', 
            '.exe', '.bin', '.iso', 
            '.css', '.js', '.json', '.xml', '.csv'
        ]
        PASSWORD_BLACKLIST = [
            'login', 'logout', 'register', 'signup', 'sign-up', 'signout', 'sign-out', 
            'password', 'email', 'username', 'user', 'pass', 'pwd', 'auth', 'token', 'session', 'cookie'
        ]

        # Check various blacklist conditions
        if 'cgi-bin' in url:
            return True
        if any(password in url.lower() for password in PASSWORD_BLACKLIST):
            return True
        for prefix in PREFIX_BLACKLIST:
            if url.startswith(prefix):
                return True
        for suffix in SUFFIX_BLACKLIST:
            if url.endswith(suffix):
                return True
        return False
    
    def is_domain_allowed_by_robots(self, url):
        """
        Check if the given URL is allowed to be crawled according to the site's robots.txt file.
        
        :param url: URL to check
        :return: Boolean indicating whether the URL is allowed to be crawled
        """
        rp = urllib.robotparser.RobotFileParser()
        robots_url = urllib.parse.urljoin(url, '/robots.txt')
        rp.set_url(robots_url)
        try:
            rp.read()
            return rp.can_fetch('*', url)
        except Exception as e:
            # If there's an error fetching robots.txt, assume it's allowed
            # logger.error(f"Error fetching robots.txt for {url}: {e}")
            return True
        
    def getDomain(self, url):
        """
        Get the domain name of the given URL with prefix.
        
        :param url: URL to extract domain from
        :return: Domain name with prefix
        """
        domain = urllib.parse.urlparse(url).netloc
        domain = "https://" + domain
        return domain
    
    def is_domain_visited(self, url):
        """
        Check if the domain of the given URL has been visited.
        
        :param url: URL to check
        :return: Boolean indicating whether the domain has been visited
        """
        domain = urllib.parse.urlparse(url).netloc
        return domain in self.visited_urls
    
    def is_url_visited(self, url):
        """
        Check if the given URL has been visited.
        
        :param url: URL to check
        :return: Boolean indicating whether the URL has been visited
        """
        domain = urllib.parse.urlparse(url).netloc
        return domain in self.visited_urls and url in self.visited_urls[domain]
    
    def add_visited_url(self, url):
        """
        Mark the given URL as visited.
        
        :param url: URL to mark as visited
        """
        domain = urllib.parse.urlparse(url).netloc
        if domain not in self.visited_urls_lock:
            self.visited_urls_lock[domain] = threading.Lock()
        with self.visited_urls_lock[domain]:
            if domain not in self.visited_urls:
                self.visited_urls[domain] = set()
            self.visited_urls[domain].add(url)

    def normalize_url(self, url):
        """
        Normalize the given URL by removing fragments, queries, and unnecessary parts.
        
        :param url: URL to normalize
        :return: Normalized URL
        """
        parsed = urllib.parse.urlparse(url)
        # Remove fragments (#), queries (?), and unnecessary part (%)
        normalized = parsed._replace(fragment='', query='')
        normalized = normalized._replace(path=re.split('%', normalized.path)[0])
        # Omit the last part of the path if it's an index or main or default page
        normalized = normalized._replace(path=re.sub(r'/(index|main|default)\.(html?|php|jsp|aspx?)$', '', normalized.path))
        # Remove the trailing slash
        if normalized.path.endswith('/'):
            normalized = normalized._replace(path=normalized.path[:-1])
        normalized_url = urllib.parse.urlunparse(normalized)
        return normalized_url

    def check_page_mime_type(self, response):
        """
        Check if the page MIME type is HTML.
        
        :param response: HTTP response object
        :return: Boolean indicating whether the page is HTML
        """
        content_type = response.getheader('Content-Type')
        return content_type and 'text/html' in content_type

    def check_url_forwarding(self, response):
        """
        Check if the URL has been forwarded and return the final URL.
        
        :param response: HTTP response object
        :return: Final URL after potential forwarding
        """
        response_url = response.geturl()
        return self.normalize_url(response_url)
    
    def fetch_page(self, url):
        """
        Fetch the content of a given URL.
        
        :param url: URL to fetch
        :return: Tuple of (response object, HTML content) or (None, None) if failed
        """
        # Define a user agent to avoid being blocked by some websites
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'}
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                html = response.read()
                return response, html
        except urllib.error.HTTPError as e:
            if e.code >= 400 and e.code < 500:
                with self.stats['4XX_errors_lock']:
                    self.stats['4XX_errors'] += 1
            elif e.code >= 500 and e.code < 600:
                with self.stats['5XX_errors_lock']:
                    self.stats['5XX_errors'] += 1
            # logger.error(f"Failed to fetch URL {url}: {e.code}, {e.reason} - Discarding this page")
            return None, None
        except Exception as e:
            # logger.error(f"Error fetching URL {url}: {e}")
            return None, None
        
    def extract_urls(self, html, base_url):
        """
        Extract all URLs from the given HTML content.
        
        :param html: HTML content to parse
        :param base_url: Base URL for resolving relative URLs
        :return: Set of extracted URLs
        """
        soup = BeautifulSoup(html, 'html.parser', from_encoding='iso-8859-1')
        base_tag = soup.find('base', href=True)
        if base_tag:
            base_url = base_tag['href']
            base_url = self.normalize_url(base_url)
        
        urls = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urllib.parse.urljoin(base_url, href)
            normalized_full_url = self.normalize_url(full_url)
            urls.add(normalized_full_url)
        return urls
    
    def enqueue_urls(self, urls, depth):
        """
        Add extracted URLs to the crawling queue with appropriate priorities.
        
        :param urls: Set of URLs to enqueue
        :param depth: Current crawling depth
        """
        for candidate_url in urls:
            if (not self.is_nz_domain(candidate_url) or self.is_blacklisted_url(candidate_url) or self.is_url_visited(candidate_url)):
                continue
            
            if not self.is_domain_visited(candidate_url):
                self.queue.put((PRIORITY_HIGH, (self.getDomain(candidate_url), 0)))
            if depth < 3:
                self.queue.put((PRIORITY_MEDIUM, (candidate_url, depth + 1)))
            elif depth < 10:
                self.queue.put((PRIORITY_LOW, (candidate_url, depth + 1)))
            else:
                self.queue.put((PRIORITY_VERY_LOW, (candidate_url, depth + 1)))
    
    def update_stats(self, size):
        """
        Update the crawling statistics with the size of the crawled page.

        :param size: Size of the crawled page
        """
        with self.stats['pages_crawled_lock']:
            self.stats['pages_crawled'] += 1
        with self.stats['total_size_lock']:
            self.stats['total_size'] += size


    def crawl_url(self, priority, url, depth):
        """
        Crawl a single URL, extract its content, and process found URLs.
        
        :param priority: Priority of the current URL
        :param url: URL to crawl
        :param depth: Current crawling depth
        """
        try:
            normalized_url = self.normalize_url(url)
            if not self.is_domain_allowed_by_robots(normalized_url):
                return
            
            response, html = self.fetch_page(normalized_url)
            if not response or not html:
                return
            
            if not self.check_page_mime_type(response):
                return
            
            normalized_final_url = self.check_url_forwarding(response)
            if (not self.is_nz_domain(normalized_final_url) or self.is_blacklisted_url(normalized_final_url) or self.is_url_visited(normalized_final_url)):
                return
            
            self.add_visited_url(normalized_final_url)
            urls = self.extract_urls(html, normalized_final_url)
            self.enqueue_urls(urls, depth)

            self.update_stats(len(html))

            self.output_log(priority, normalized_final_url, depth, len(html), response.getcode())

        except Exception as e:
            # logger.error(f"Error crawling URL {url}: {e}")
            pass
    
    def output_log(self, priority, url, depth, size, code):
        """
        Log the results of crawling a URL.
        
        :param priority: Priority of the crawled URL
        :param url: Crawled URL
        :param depth: Depth at which the URL was crawled
        :param size: Size of the crawled page
        :param code: HTTP status code of the response
        """
        output = [
            f'{datetime.now():%Y-%m-%d %H:%M:%S.%f}',
            f'{priority}',
            f'{depth}',
            f'{size}',
            f'{code}',
            f'{url}'
        ]
        logger.info('\t'.join(output))

class Crawler():
    """
    Main Crawler class that manages the crawling process.
    """

    def __init__(self, seed_urls, num_threads):
        """
        Initialize the Crawler.
        
        :param seed_urls: List of initial URLs to start crawling from
        :param num_threads: Number of worker threads to use
        """
        self.seed_urls = seed_urls
        self.queue = PriorityQueue()
        for url in seed_urls:
            self.queue.put((PRIORITY_VERY_HIGH, (url, 0)))
        self.visited_urls = {}
        self.visited_urls_lock = {}
        self.stats = {
            'pages_crawled': 0,
            'total_size': 0,
            '4XX_errors': 0,
            '5XX_errors': 0,
            'pages_crawled_lock': threading.Lock(),
            'total_size_lock': threading.Lock(),
            'total_size_lock': threading.Lock(),
            '4XX_errors_lock': threading.Lock(),
            '5XX_errors_lock': threading.Lock(),
        }
        self.start_time = None
        self.end_time = None
        self.num_threads = num_threads
        self.workers = []
    
    def crawl(self):
        """
        Start the crawling process by initializing and starting worker threads.
        """
        logger.info('Starting...')
        self.start_time = datetime.now()
        logger.info('Timestamp\tPriority\tDepth\tSize\tCode\tURL')
        signal.signal(signal.SIGINT, self.handle_terminate)
        for _ in range(self.num_threads):
            worker = Worker(self.queue, self.visited_urls, self.visited_urls_lock, self.stats)
            worker.start()
            self.workers.append(worker)
        
        for worker in self.workers:
            worker.join()
        self.queue.join()

        self.log_stats()

    def log_stats(self):
        """
        Log the final statistics of the crawling process.
        """
        self.end_time = datetime.now()
        final_log_message = (
            f"Total pages crawled: {self.stats['pages_crawled']}, "
            f"Total size: {self.stats['total_size']}, "
            f"4XX errors: {self.stats['4XX_errors']}, "
            f"5XX errors: {self.stats['5XX_errors']}, "
            f"Start time: {self.start_time:%Y-%m-%d %H:%M:%S.%f}, "
            f"End time: {self.end_time:%Y-%m-%d %H:%M:%S.%f}, "
            f"Total time: {self.end_time - self.start_time}"
        )
        logger.info(final_log_message)
        logger.info('Exiting...')


    def handle_terminate(self, signum, frame):
        """
        Handle the exit signal (SIGINT) by logging final statistics and exiting.
        
        :param signum: Signal number
        :param frame: Current stack frame
        """
        self.log_stats()
        sys.exit(0)

if __name__ == "__main__":
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description='Primitive Web Crawler: This script crawls a set of seed URLs with multithreading and logs the results.')
    parser.add_argument('--seeds', type=str, default="seeds.txt", help='Path to the file where the randomly selected seed URLs will be written. Default is seeds.txt.')
    parser.add_argument('--crawl_log', type=str, default="crawl_log.txt", help='Path to the log file where crawl results will be saved. Default is crawl_log.txt.')
    parser.add_argument('--threads', type=int, default=15, help='Number of threads for crawling. Default is 15.')
    args = parser.parse_args()
    
    # Read seed URLs from file and randomly select 20
    with open('nz_domain_seeds_list.txt', 'r') as f:
        seed_urls = [url.strip() for url in f if url.strip()]
    sample_seed_urls = random.sample(seed_urls, 20)
    
    # Write selected seed URLs to file
    with open(args.seeds, 'w') as f:
        for url in sample_seed_urls:
            f.write(url + '\n')

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            logging.FileHandler(args.crawl_log),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    # Initialize and start the crawler
    crawler = Crawler(sample_seed_urls, num_threads=args.threads)
    crawler.crawl()
