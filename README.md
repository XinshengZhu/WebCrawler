# Primitive Web Crawler

## Files

- `crawl_log1.txt`: Output log file for `seeds1.txt`, each of which lists all pages crawled by the crawler.
- `crawl_log2.txt`: Output log file for `seeds2.txt`, each of which lists all pages crawled by the crawler.
- `crawler.py`: Well-commented Python program source code for the primitive web crawler.
- `explain.txt`: A precise and succinct description of the crawler's features and how the program works.
- `readme.txt`: ASCII form file with a short description of the submitted content.
- `seeds1.txt`: A randomly selected subset of seeds from `nz_domain_seeds_list.txt`, containing 20 seed pages.
- `seeds2.txt`: Another randomly selected subset of seeds from `nz_domain_seeds_list.txt`, containing 20 seed pages.

## Requirements
- `nz_domain_seeds_list.txt` should be placed in the same level directory as `crawler.py`.

- Python 3.12.5 environment is recommended and the  `BeautifulSoap` library should be installed.

  ```bash
  $ pip install beautifulsoup4
  ```

## Instructions

* Move to the `path` where `crawler.py` and ``nz_domain_seeds_list.txt`` are stored.

  ```bash
  $ cd [path]
  ```

* Check the usage of the script `crawler.py`

  ```bash
  $ python3 crawler.py -h
  usage: crawler.py [-h] [--seeds SEEDS] [--crawl_log CRAWL_LOG] [--threads THREADS]
  
  Primitive Web Crawler: This script crawls a set of seed URLs with multithreading and logs the results.
  
  options:
    -h, --help            show this help message and exit
    --seeds SEEDS         Path to the file where the randomly selected seed URLs will be written. Default is seeds.txt.
    --crawl_log CRAWL_LOG
                          Path to the log file where crawl results will be saved. Default is crawl_log.txt.
    --threads THREADS     Number of threads for crawling. Default is 15.
  ```

* Example: Save the randomly selected subset of seeds in `seeds.txt`, based on which start the crawler with 15 threads, and log important crawling information to `crawl_log.txt` in real-time.

  ```bash
  $ python3 crawler.py --seeds "seeds.txt" --crawl_log "crawl_log.txt" --threads 15
  ```

* Press `CTRL+C` to terminate the crawler whenever you want
