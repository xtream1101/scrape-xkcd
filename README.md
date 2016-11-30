# scrape-xkcd

Developed using Python 3.4

Must pass in a config file like so: `python3 xkcd-comics.py -c ~/scrapers.conf`

See what the conf file need to be here: https://github.com/xtream1101/scraper-lib

## Setup

Run `pip3 install -r requirements.txt`

## Comics
Scrape the site http://xkcd.com/ and save all the comics on the site and get new ones on each run.

This scraper also requires the section in the config:
```
[xkcd-comics]
# `scraper_key` is only needed if `scraper-monitor` is enabled
scraper_key =
```


## What If
Scrape the site http://what-if.xkcd.com/ and save all the _What if's_ on the site and get new ones on each run.

Needs `phantomjs` installed on the computer that is running the scraper.

This scraper also requires the section in the config:
```
[xkcd-whatif]
# `scraper_key` is only needed if `scraper-monitor` is enabled
scraper_key =
```
