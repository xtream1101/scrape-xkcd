# scrape-xkcd

Developed using Python 3.4

Scrape the site http://xkcd.com/ and save all the comics on the site and get missing ones on each run.

Must pass in a config file like so: `python3 xkcd-comics.py -c ~/scrapers.conf`

See what the conf file need to be here: https://git.eddyhintze.com/xtream1101/scraper-lib

This scraper also requires the section in the config:
```
[xkcd-comics]
# `scraper_key` is only needed if `scraper-monitor` is enabled
scraper_key =
```

Requried:

    - Postgres database with a schema `xkcd`
