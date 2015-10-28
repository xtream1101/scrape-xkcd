import os
import sys
import signal
from custom_utils.custom_utils import CustomUtils
from custom_utils.exceptions import *
from custom_utils.sql import *


class Xkcd(CustomUtils):

    def __init__(self, base_dir, url_header=None):
        super().__init__()
        # Make sure base_dir exists and is created
        self._base_dir = base_dir

        # Set url_header
        self._url_header = self._set_url_header(url_header)

        # Setup database
        self._db_setup()

        # Start parsing the site
        self.start()

    def start(self):
        latest = self.get_latest()
        progress = self.sql.get_progress()

        if latest == progress:
            # Nothing new to get
            self.cprint("Already have the latest")
            return

        for i in range(progress + 1, latest + 1):
            self.cprint("Getting comic: " + str(i))
            self.parse(i)
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
            data = self.get_site(url, self._url_header, is_json=True)
        except RequestsError:
            # TODO: Do something more useful here i.e. let the user know and do not just start at 0
            return 0
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
            return

        url = "https://xkcd.com/" + str(id_) + "/info.0.json"
        try:
            prop = self.get_site(url, self._url_header, is_json=True)
        except RequestsError:
            # TODO: Do something more useful here i.e. let the user know
            return
        # prop Needs an id
        prop['id'] = str(prop['num'])

        # #####
        # # Download images
        # #####
        file_ext = self.get_file_ext(prop['img'])
        file_name = prop['id']
        prop['save_path'] = self._base_dir + "/" + prop['id'][-1] + "/"
        prop['save_path'] += self.sanitize(file_name) + file_ext
        if self.download(prop['img'], prop['save_path'], self._url_header):
            self._save_meta_data(prop)

        # Everything was successful
        return True

    def _set_url_header(self, url_header):
        if url_header is None:
            # Use default from CustomUtils
            return self.get_default_header()
        else:
            return url_header

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
    if len(sys.argv) < 2:
        print("You must pass in the save directory of the scraper")

    save_dir = CustomUtils().create_path(sys.argv[1], is_dir=True)
    # Start the scraper
    scrape = Xkcd(save_dir)

    print("")
