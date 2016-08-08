import os
import sys
import time
import json
import signal
import logging
import argparse
import logging.handlers
from scraper_monitor import scraper_monitor
import custom_utils as cutil
from scrapers import Scraper, Web

# This will allow you to run the scraper in `DEV` mode or `PROD` by passing `-e dev`.
# If nothing is passed it will run in `DEV` mode
parser = argparse.ArgumentParser(description='Scraper')
parser.add_argument('-e', '--environment', help='Environment to run in: PROD | DEV (default).',
                    nargs='?', default='DEV')
parser.add_argument('-c', '--config', help='Config file. Default `None`',
                    nargs='?', default=None)
args = parser.parse_args()

RUN_SCRAPER_AS = args.environment.upper()

if RUN_SCRAPER_AS not in ['DEV', 'PROD']:
    print("You must set the env var RUN_SCRAPER_AS to DEV or PROD")
    sys.exit(1)

# ALWAYS use UTC time for your scraper. That way all data is consistent no matter where it is running from
os.environ['TZ'] = 'UTC'

# Send logs to scraper monitor
scraper_name = cutil.get_script_name(ext=False)
# Scraper key as found in the Scraper Monitor v2
scraper_key = 'Ga1eJdR5'  # EDIT
host = "scrapermonitor-2014968964.us-east-1.elb.amazonaws.com:80"  # EDIT
# API key as found in the Scraper Monitor v2
apikey = '57815a2c4cfe467c97410ed41b778f9c'  # EDIT
scrape_id = cutil.create_uid()

url = "api/v1/logs?apikey={apikey}&scraperKey={scraper_key}&scraperRun={scrape_id}&environment={env}"\
      .format(apikey=apikey, scraper_key=scraper_key, scrape_id=scrape_id, env=RUN_SCRAPER_AS)

# Set global logging settings
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set logs to rotate every day
# By defaut logs will be in a `logs` folder in the users home directoy, edit to move elsewhere
# Also, inside the logs folder, they will be split up by `DEV` and `PROD` so the log messages do not get mixed
log_file = cutil.create_path('~/logs/' + RUN_SCRAPER_AS + '/' + cutil.get_script_name(ext=False) + '.log')
# New log files will be created every day to make checking logs simple
rotate_logs = logging.handlers.TimedRotatingFileHandler(log_file,
                                                        when="d",
                                                        interval=1,
                                                        backupCount=0)
# Create formatter for the rotating log files
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotate_logs.setFormatter(formatter)
logger.addHandler(rotate_logs)

# Send logs to scraper monitor
http_handler = logging.handlers.HTTPHandler(host, url, method='POST')
http_handler.setLevel(logging.WARNING)
logger.addHandler(http_handler)

# Create logger for this script
logger = logging.getLogger(__name__)
# Start/configure the scraper monitoring
scraper_monitor.start(scraper_name, host, apikey, scraper_key, scrape_id, RUN_SCRAPER_AS)


class Worker:

    def __init__(self, web, comic_id):
        """
        Worker Profile

        Run for each item that needs parsing
        Each thread has a web instance that is used for parsing
        """
        # `web` is what utilizes the profiles and proxying
        self.web = web

        # Get the sites content as a beautifulsoup object
        url = 'https://xkcd.com/{comic_id}/info.0.json'.format(comic_id=comic_id)
        response = self.web.get_site(url, page_format='json')
        logger.info("Getting comic {comic_id}-{comic_title}".format(comic_id=response.get('num'),
                                                                    comic_title=response.get('title')))

        comic_download_path = '{base_path}/{last_num}/{comic_id}{file_ext}'\
                              .format(base_path=self.web.scraper.save_path,
                                      last_num=str(response.get('num'))[-1],
                                      comic_id=response.get('num'),
                                      file_ext=cutil.get_file_ext(response.get('img'))
                                      )
        parsed_data = {'comic_id': response.get('num'),
                       'alt': response.get('alt'),
                       'image_path': self.web.download(response.get('img'), comic_download_path),
                       'posted_at': '{year}-{month}-{day}'.format(year=response.get('year'),
                                                                  month=response.get('month'),
                                                                  day=response.get('day')),
                       'time_collected': cutil.get_datetime(),
                       'title': response.get('title'),
                       'transcript': response.get('transcript'),
                       'raw_json': json.dumps(response),
                       }

        # Add raw data to db
        self.web.scraper.insert_data(parsed_data)

        # Add success count to stats. Keeps track of how much ref data has been parsed
        self.web.scraper.track_stat('ref_data_success_count', 1)

        # Take it easy on the site
        time.sleep(1)

    def parse(self, content):
        """
        :return: List of items with their details
        """
        # Parse the items here and return the content to be added to the db
        pass


class Xkcd(Scraper):

    def __init__(self, config_file=None):
        super().__init__('xkcd', scrape_id, run_scraper_as=RUN_SCRAPER_AS, config_file=config_file)  # EDIT

        # Gives access to `self.db`
        self.db_setup()

        self.save_path = self.config['global']['BASE_PATH'] + '/xkcd/comics'
        self.max_id = self.get_latest()
        self.last_id_scraped = self.get_last_scraped()

    def start(self):
        """
        Send the ref data to the worker threads
        """
        if self.max_id == self.last_id_scraped:
            # No need to continue
            logger.info("Already have the newest comic")
            return

        comic_ids = range(self.last_id_scraped + 1, self.max_id + 1)

        # Log how many items in total we will be parsing
        scraper.stats['ref_data_count'] = len(comic_ids)

        # Use `selenium` or `requests` here
        self.thread_profile(1, 'requests', comic_ids, Worker)

    def get_latest(self):
        """
        Get the latest comic id posted
        """
        tmp_web = Web(self, 'requests')

        url = "https://xkcd.com/info.0.json"
        # Get the json data
        try:
            data = tmp_web.get_site(url, page_format='json')
        except:
            logger.critical("Problem getting latest comic id", exc_info=True)
            sys.exit(1)

        max_id = int(data.get('num'))
        logger.info("Newest upload: {id}".format(id=max_id))

        return max_id

    def get_last_scraped(self):
        """
        Get last comic scraped
        """
        last_scraped_id = None
        with self.db.getcursor() as cur:
            cur.execute("""SELECT comic_last_id FROM xkcd.setting WHERE bit=0""")

            last_scraped_id = cur.fetchone()[0]

        if last_scraped_id is None:
            last_scraped_id = 0

        return last_scraped_id

    def log_last_scraped(self):
        try:
            with self.db.getcursor() as cur:
                # Get last id in db
                cur.execute("SELECT comic_id FROM xkcd.comic ORDER BY comic_id DESC")
                last_comic = cur.fetchone()[0]
                # Log that id
                cur.execute("""UPDATE xkcd.setting
                               SET comic_last_id=%(last_id)s, comic_last_ran=%(timestamp)s
                               WHERE bit=0""", {'last_id':last_comic, 'timestamp':cutil.get_datetime()})
        except:
            logger.exception("Problem logging last comic scraped")


    def insert_data(self, data):
        """
        Will handle inserting data into the database
        """
        # TODO: Make UPSERT
        try:
            with self.db.getcursor() as cur:
                cur.execute("""INSERT INTO xkcd.comic
                               (alt, comic_id, image_path, posted_at, raw_json, time_collected, title, transcript)
                               VALUES
                               (%(alt)s, %(comic_id)s, %(image_path)s, %(posted_at)s, %(raw_json)s,
                                %(time_collected)s, %(title)s, %(transcript)s)""",
                               data)

                # Log how many rows were added
                rows_affected = cur.rowcount
                self.track_stat('rows_added_to_db', rows_affected)

        except Exception:
            logger.exception("Error adding to db {}".format(data))


def sigint_handler(signal, frame):
    logger.critical("Keyboard Interrupt")
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    # Use the correct config file
    if args.config is not None:
        config_file = args.config
    elif RUN_SCRAPER_AS == 'DEV':
        config_file = '/etc/scraper/scraper_dev.conf'
    elif RUN_SCRAPER_AS == 'PROD':
        config_file = '/etc/scraper/scraper.conf'

    try:
        scraper = Xkcd(config_file=config_file)
        try:
            scraper.start()
            scraper.cleanup()

        except Exception:
            logger.critical("Main Error", exc_info=True)

    except Exception:
        logger.critical("Setup Error", exc_info=True)

    finally:
        scraper.log_last_scraped()
        try:
            # Log stats
            scraper_monitor.stop(total_urls=scraper.stats['total_urls'],
                                 ref_data_count=scraper.stats['ref_data_count'],
                                 ref_data_success_count=scraper.stats['ref_data_success_count'],
                                 rows_added_to_db=scraper.stats['rows_added_to_db'])

        except NameError:
            # If there is an issue with scraper.stats
            scraper_monitor.stop()

        except Exception:
            logger.critical("Scraper Monitor Stop Error", exc_info=True)
            scraper_monitor.stop()
