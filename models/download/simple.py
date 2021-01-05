from threading import Thread
from datetime import datetime

import os

from models.ftp import Ftp
from models.files import Files
from models.config import Config
from models.databases import Database

class Simple:

    config = Config()

    ARCHIVE = config.get('SIMPLE FILES', 'archive')
    THREADS = int(config.get('SIMPLE FILES', 'processes'))
    ROOT_PATH = config.get('SIMPLE FILES', 'path')

    def __init__(self, filelist, scanners):
        self.filelist = filelist
        self.scanners = scanners
        
        # Get current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.path = self.ROOT_PATH + current_date

        self.main()