# RIB-Data-Scraper
Scrapes all data from RIB to an SQLite database.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install required dependencies.

```bash
pip install sqlite3
pip install pandas
pip install beautifulsoup4

```


## Usage

Edit event_scraping.py lines 74 and 80 for the eventID you wish to scrape and the database name you want to save to respectively.

Run the script and it will create a new database if it doesn't exist and append the data scraped to said database.

