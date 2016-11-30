import sys
import time
import json
import cutil
import signal
import logging
from scraper_monitor import scraper_monitor
from models import db_session, Setting, Comic, NoResultFound
from scraper_lib import Scraper, Web

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
        else:
            parsed_data = self.parse(response)
            if len(parsed_data) > 0:
                # Add raw data to db
                self.web.scraper.insert_data(parsed_data)

                # Remove id from list of comics to get
                self.web.scraper.comic_ids.remove(comic_id)

                # Add success count to stats. Keeps track of how much ref data has been parsed
                self.web.scraper.track_stat('ref_data_success_count', 1)

        # Take it easy on the site
        time.sleep(1)

    def parse(self, response):
        """
        :return: Dict the content
        """
        rdata = {}
        # Parse the items here and return the content to be added to the db
        logger.info("Getting comic {comic_id}-{comic_title}".format(comic_id=response.get('num'),
                                                                    comic_title=response.get('title')))

        comic_filename = '{last_num}/{comic_id}{file_ext}'\
                         .format(last_num=str(response.get('num'))[-1],
                                 comic_id=response.get('num'),
                                 file_ext=cutil.get_file_ext(response.get('img'))
                                 )
        rdata = {'comic_id': response.get('num'),
                 'alt': response.get('alt'),
                 'file_path': self.web.download(response.get('img'), comic_filename),
                 'posted_at': '{year}-{month}-{day}'.format(year=response.get('year'),
                                                            month=response.get('month'),
                                                            day=response.get('day')),
                 'time_collected': cutil.get_datetime(),
                 'title': response.get('title'),
                 'transcript': response.get('transcript'),
                 'raw_json': json.dumps(response),
                 }

        return rdata


class XkcdComics(Scraper):

    def __init__(self, config_file=None):
        super().__init__('xkcd')

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

        # Only ever use 1 thread here
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
        last_scraped_id = db_session.query(Setting).filter(Setting.bit == 0).one().comic_last_id

        if last_scraped_id is None:
            last_scraped_id = 0

        return last_scraped_id

    def log_last_scraped(self):
        try:
            # Find the lowest comic id we did not scrape yet and start there next time
            if 404 in self.comic_ids:
                # This is never successful because it always returns a 404 page
                self.comic_ids.remove(404)
            try:
                last_comic_id = min(self.comic_ids) - 1
            except ValueError:
                last_comic_id = self.max_id

            setting = db_session.query(Setting).filter(Setting.bit == 0).one()
            setting.comic_last_id = last_comic_id
            setting.comic_last_ran = cutil.get_datetime()

            db_session.add(setting)
            db_session.commit()

        except:
            logger.exception("Problem logging last comic scraped")

    def insert_data(self, data):
        """
        Will handle inserting data into the database
        """
        try:
            # Check if comic is in database, if so update else create
            try:
                comic = db_session.query(Comic).filter(Comic.comic_id == data.get('comic_id')).one()
            except NoResultFound:
                comic = Comic()

            comic.title = data.get('title')
            comic.alt = data.get('alt')
            comic.comic_id = data.get('comic_id')
            comic.file_path = data.get('file_path')
            comic.posted_at = data.get('posted_at')
            comic.raw_json = data.get('raw_json')
            comic.time_collected = data.get('time_collected')
            comic.transcript = data.get('transcript')

            db_session.add(comic)
            db_session.commit()
            # self.track_stat('rows_added_to_db', rows_affected)

        except Exception:
            db_session.rollback()
            logger.exception("Error adding to db {data}".format(data=data))


def sigint_handler(signal, frame):
    logger.critical("Keyboard Interrupt")
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    try:
        # Setup the scraper
        scraper = XkcdComics()
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
