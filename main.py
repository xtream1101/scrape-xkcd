import os
import sys
import yaml
import signal
import argparse
from custom_utils.custom_utils import CustomUtils
from custom_utils.exceptions import *
from custom_utils.sql import *

# Set timezone to UTC
os.environ['TZ'] = 'UTC'


class Xkcd(CustomUtils):

    def __init__(self, base_dir, restart=False, proxies=[], url_header=None):
        super().__init__()
        # Make sure base_dir exists and is created
        self._base_dir = base_dir

        # Do we need to restart
        self._restart = restart

        # Set url_header
        self._url_header = self.set_url_header(url_header)

        # If we have proxies then add them
        if len(proxies) > 0:
            self.set_proxies(proxies)
            self.log("Using IP: " + self.get_current_proxy())

        # Setup database
        self._db_setup()

        # Start parsing the site
        self.start()

    def start(self):
        latest = self.get_latest()

        if self._restart is True:
            progress = 0
        else:
            progress = self.sql.get_progress()

        if latest == progress:
            # Nothing new to get
            self.cprint("Already have the latest")
            return

        for i in range(progress + 1, latest + 1):
            self.cprint("Getting comic: " + str(i))
            if self._restart is True:
                check_data = self._db_session.query(Data).filter(Data.id == i).first()
                if check_data is not None:
                    continue

            if self.parse(i) is not False:
                self.sql.update_progress(i)

    def get_latest(self):
        """
        Uses xkcd's api at https://xkcd.com/json.html
        :return: id of the newest item
        """
        self.cprint("##\tGetting newest upload id...\n")

        url = "https://xkcd.com/info.0.json"
        # Get the json data
        try:
            data = self.get_site(url, self._url_header, page_format='json')
        except RequestsError as e:
            print("Error getting latest: " + str(e))
            sys.exit(0)

        max_id = data['num']
        self.cprint("##\tNewest upload: " + str(max_id) + "\n")
        return int(max_id)

    def parse(self, id_):
        """
        Using the json api, get the comic and its info
        :param id_: id of the comic on `http://xkcd.com`
        :return:
        """
        # There is no 0 comic
        # 404 does not exists (this is the joke)
        if id_ == 0 or id_ == 404:
            return False

        url = "https://xkcd.com/" + str(id_) + "/info.0.json"
        try:
            prop = self.get_site(url, self._url_header, page_format='json')
        except RequestsError as e:
            print("Error getting (" + url + "): " + str(e))
            return False

        # prop Needs an id
        prop['id'] = str(prop['num'])

        #####
        # Download comics
        #####
        file_ext = self.get_file_ext(prop['img'])
        file_name = prop['id']
        prop['save_path'] = self._base_dir + "/" + prop['id'][-1] + "/"
        prop['save_path'] += self.sanitize(file_name) + file_ext

        if self.download(prop['img'], prop['save_path'], self._url_header):
            self._save_meta_data(prop)

        # Everything was successful
        return True

    def _save_meta_data(self, data):
        xkcd_data = Data(num=data['num'],
                         added_utc=self.get_utc_epoch(),
                         day=data['day'],
                         month=data['month'],
                         year=data['year'],
                         alt=data['alt'],
                         transcript=data['transcript'],
                         news=data['news'],
                         img=data['img'],
                         link=data['link'],
                         title=data['title'],
                         safe_title=data['safe_title']
                         )
        self._db_session.add(xkcd_data)

        try:
            self._db_session.commit()
        except sqlalchemy.exc.IntegrityError:
            # tried to add an item to the database which was already there
            pass

    def _db_setup(self):
        # Version of this database
        db_version = 1
        db_file = os.path.join(self._base_dir, "xkcd.sqlite")
        self.sql = Sql(db_file, db_version)
        is_same_version = self.sql.set_up_db()
        if not is_same_version:
            # Update database to work with current version
            pass

        # Get session
        self._db_session = self.sql.get_session()


class Data(Base):
    __tablename__ = 'data'
    num        = Column(Integer, primary_key=True)
    added_utc  = Column(Integer, nullable=False)
    day        = Column(Integer, nullable=False)
    month      = Column(Integer, nullable=False)
    year       = Column(Integer, nullable=False)
    alt        = Column(String,  nullable=False)
    transcript = Column(String,  nullable=False)
    news       = Column(String,  nullable=False)
    img        = Column(String(150), nullable=False)
    link       = Column(String(150), nullable=False)
    title      = Column(String(100), nullable=False)
    safe_title = Column(String(100), nullable=False)


def signal_handler(signal, frame):
    print("")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    # Deal with args
    parser = argparse.ArgumentParser(description='Scrape site and archive data')
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('-d', '--dir', help='Absolute path to save directory')
    parser.add_argument('-r', '--restart', help='Set to start parsing at 0', action='store_true')
    args = parser.parse_args()

    # Set defaults
    save_dir = None
    restart = None
    proxy_list = []

    if args.config is not None:
        # Load config values
        if not os.path.isfile(args.config):
            print("No config file found")
            sys.exit(0)

        with open(args.config, 'r') as stream:
            config = yaml.load(stream)

        # Check config file first
        if 'save_dir' in config:
            save_dir = config['save_dir']
        if 'restart' in config:
            restart = config['restart']

        # Proxies can only be set via config file
        if 'proxies' in config:
            proxy_list = config['proxies']

    # Command line args will overwrite config args
    if args.dir is not None:
        save_dir = args.dir

    if restart is None or args.restart is True:
        restart = args.restart

    # Check to make sure we have our args
    if args.dir is None and save_dir is None:
        print("You must supply a config file with `save_dir` or -d")
        sys.exit(0)

    save_dir = CustomUtils().create_path(save_dir, is_dir=True)

    # Start the scraper
    scrape = Xkcd(save_dir, restart=restart, proxies=proxy_list)

    print("")
