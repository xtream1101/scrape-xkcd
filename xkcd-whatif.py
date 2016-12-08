import sys
import time
import cutil
import signal
import logging
from scraper_monitor import scraper_monitor
from models import db_session, Setting, Whatif, NoResultFound
from scraper_lib import Scraper
from web_wrapper import DriverSeleniumPhantomJS, DriverRequests

# Create logger for this script
logger = logging.getLogger(__name__)


class Worker:

    def __init__(self, scraper, web, whatif_id):
        """
        Worker Profile

        Run for each item that needs parsing
        Each thread has a web instance that is used for parsing
        """
        # `web` is what utilizes the profiles and proxying
        self.web = web
        self.scraper = scraper
        self.whatif_id = whatif_id

        # Get the sites content as a beautifulsoup object
        logger.info("Getting what if {id}".format(id=self.whatif_id))
        url = "http://what-if.xkcd.com/{id}/".format(id=self.whatif_id)
        response = self.web.get_site(url, page_format='html')
        if response is None:
            logger.warning("Response was None for url {url}".format(url=url))

        else:
            parsed_data = self.parse(response)
            if len(parsed_data) > 0:
                # Add raw data to db
                self.scraper.insert_data(parsed_data)

                # Remove id from list of comics to get
                self.scraper.whatif_ids.remove(self.whatif_id)

                # Add success count to stats. Keeps track of how much ref data has been parsed
                self.scraper.track_stat('ref_data_success_count', 1)

        # Take it easy on the site
        time.sleep(1)

    def parse(self, soup):
        """
        :return: List of items with their details
        """
        rdata = self.scraper.archive_list.get(self.whatif_id)

        # Parse the items here and return the content to be added to the db
        article = self.web.driver.find_element_by_css_selector('article.entry')

        rdata['question'] = soup.find('article', {'class': 'entry'}).find('p', {'id': 'question'}).get_text()

        whatif_filename = '{base}/{last_num}/{whatif_id}.png'\
                          .format(base=self.scraper.BASE_SAVE_DIR,
                                  last_num=str(self.whatif_id)[-1],
                                  whatif_id=self.whatif_id)

        rdata.update({'whatif_id': self.whatif_id,
                      'saved_file_location': self.web.screenshot(whatif_filename, element=article),
                      'time_collected': cutil.get_datetime(),
                      })

        return rdata


class XkcdWhatif(Scraper):

    def __init__(self, config_file=None):
        super().__init__('xkcd')

        self.archive_list = self.load_archive_list()
        self.max_id = self.get_latest()
        self.last_id_scraped = self.get_last_scraped()
        self.whatif_ids = []

    def start(self):
        """
        Send the ref data to the worker threads
        """
        if self.max_id == self.last_id_scraped:
            # No need to continue
            logger.info("Already have the newest whatif")
            return

        self.whatif_ids = list(range(self.last_id_scraped + 1, self.max_id + 1))

        # Log how many items in total we will be parsing
        scraper.stats['ref_data_count'] = len(self.whatif_ids)

        # Only ever use 1 thread here
        self.thread_profile(1, DriverSeleniumPhantomJS, self.whatif_ids, Worker)

    def load_archive_list(self):
        """
        Load all the whatifs and store in a dict with the id's as keys
        Need to do this since this is the only place where the date posted is listed
        """
        rdata = {}
        tmp_web = DriverRequests()

        url = "http://what-if.xkcd.com/archive/"
        try:
            soup = tmp_web.get_site(url, page_format='html')
        except RequestsError as e:
            logger.critical("Problem getting whatif archive", exc_info=True)
            sys.exit(1)

        entries = soup.find_all('div', {'class': 'archive-entry'})

        for entry in entries:
            try:
                _id = int(entry.find('a')['href'].split('/')[-2])
                title = entry.find(class_='archive-title').text
                posted_at = entry.find(class_='archive-date').text

                rdata[_id] = {'posted_at': posted_at,
                              'title': title,
                              }
            except (AttributeError, ValueError):
                logger.critical("Cannot parse data for entry {entry}".format(entry=str(entry)))

        return rdata

    def get_latest(self):
        """
        Get the latest whatif id posted
        """
        max_id = max(self.archive_list.keys())
        logger.info("Newest upload: {id}".format(id=max_id))

        return max_id

    def get_last_scraped(self):
        """
        Get last whatif scraped
        """
        last_scraped_id = db_session.query(Setting).filter(Setting.bit == 0).one().whatif_last_id

        if last_scraped_id is None:
            last_scraped_id = 0

        return last_scraped_id

    def log_last_scraped(self):
        try:
            try:
                last_whatif_id = min(self.whatif_ids) - 1
            except ValueError:
                last_whatif_id = self.max_id

            setting = db_session.query(Setting).filter(Setting.bit == 0).one()
            setting.whatif_last_id = last_whatif_id
            setting.whatif_last_ran = cutil.get_datetime()

            db_session.add(setting)
            db_session.commit()

        except:
            logger.exception("Problem logging last whatif scraped")

    def insert_data(self, data):
        """
        Will handle inserting data into the database
        """
        try:
            # Check if whatif is in database, if so update else create
            try:
                whatif = db_session.query(Whatif).filter(Whatif.whatif_id == data.get('whatif_id')).one()
            except NoResultFound:
                whatif = Whatif()

            whatif.title = data.get('title')
            whatif.question = data.get('question')
            whatif.whatif_id = data.get('whatif_id')
            whatif.saved_file_location = data.get('saved_file_location')
            whatif.posted_at = data.get('posted_at')
            whatif.time_collected = data.get('time_collected')

            db_session.add(whatif)
            db_session.commit()

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
        scraper = XkcdWhatif()
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
