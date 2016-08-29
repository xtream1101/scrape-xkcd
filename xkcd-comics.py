import os
import sys
import time
import json
import signal
import logging
from scraper_monitor import scraper_monitor
from models import db_session, Comics
import custom_utils as cutil
from scrapers import Scraper, Web, args, RUN_SCRAPER_AS, config, SCRAPE_ID

# Create logger for this script
logger = logging.getLogger(__name__)


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
        if response is None:
            logger.warning("Response was None for url {url}".format(url=url))
            return

        logger.info("Getting comic {comic_id}-{comic_title}".format(comic_id=response.get('num'),
                                                                    comic_title=response.get('title')))

        comic_filename = '{last_num}/{comic_id}{file_ext}'\
                         .format(last_num=str(response.get('num'))[-1],
                                 comic_id=response.get('num'),
                                 file_ext=cutil.get_file_ext(response.get('img'))
                                 )
        parsed_data = {'comic_id': response.get('num'),
                       'alt': response.get('alt'),
                       'image_path': self.web.download(response.get('img'), comic_filename),
                       'posted_at': '{year}-{month}-{day}'.format(year=response.get('year'),
                                                                  month=response.get('month'),
                                                                  day=response.get('day')),
                       'time_collected': cutil.get_datetime(),
                       'title': response.get('title'),
                       'transcript': response.get('transcript'),
                       'raw_json': json.dumps(response),
                       }
        new_comic = Comics()
        new_comic.name = response.get('title')
        db_session.add(new_comic)
        db_session.commit()


        # Add raw data to db
        self.web.scraper.insert_data(parsed_data)

        # Remove id from list of comics to get
        self.web.scraper.comic_ids.remove(comic_id)

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
        super().__init__('xkcd')

        # Gives access to `self.db`
        self.db_setup()

        self.max_id = self.get_latest()
        self.last_id_scraped = self.get_last_scraped()
        self.comic_ids = []

    def start(self):
        """
        Send the ref data to the worker threads
        """
        if self.max_id == self.last_id_scraped:
            # No need to continue
            logger.info("Already have the newest comic")
            return

        self.comic_ids = list(range(self.last_id_scraped + 1, self.max_id + 1))

        # Log how many items in total we will be parsing
        scraper.stats['ref_data_count'] = len(self.comic_ids)

        # Use `selenium` or `requests` here
        self.thread_profile(1, 'requests', self.comic_ids, Worker)

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
                # Find the lowest comic id we did not scrape yet and start there next time
                if 404 in self.comic_ids:
                    # This is never successful because it always returns a 404 page
                    self.comic_ids.remove(404)
                try:
                    last_comic = min(self.comic_ids) - 1
                except ValueError:
                    last_comic = self.max_id

                # Log that id
                cur.execute("""UPDATE xkcd.setting
                               SET comic_last_id=%(last_id)s, comic_last_ran=%(timestamp)s
                               WHERE bit=0""", {'last_id': last_comic, 'timestamp': cutil.get_datetime()})
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
            logger.exception("Error adding to db {data}".format(data=data))


def sigint_handler(signal, frame):
    logger.critical("Keyboard Interrupt")
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    try:
        # Make sure tables are created

        # Setup the scraper
        scraper = Xkcd()
        try:
            # Start scraping
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
