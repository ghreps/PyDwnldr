import configparser
import os

class Config:
    
    def __init__(self):
        if not os.path.exists('config.ini'):
            self.create_config()
        self.file = configparser.ConfigParser()
        self.file.read('config.ini')

    def get(self, section, param):
        return self.file.get(section, param)

    def create_config():
        new = configparser.ConfigParser()
        new.add_section('FTP')
        new.set('FTP', 'ip', '')
        new.set('FTP', 'user', '')
        new.set('FTP', 'pswd', '')
        new.add_section('MACS LIST')
        new.set('MACS LIST', 'lifetime', '7')
        new.add_section('CABINET SQL')
        new.set('CABINET SQL', 'host', '')
        new.set('CABINET SQL', 'port', '')
        new.set('CABINET SQL', 'user', '')
        new.set('CABINET SQL', 'pswd', '')
        new.set('CABINET SQL', 'db', '')
        new.add_section('STORAGE SQL')
        new.set('STORAGE SQL', 'host', '')
        new.set('STORAGE SQL', 'port', '')
        new.set('STORAGE SQL', 'user', '')
        new.set('STORAGE SQL', 'pswd', '')
        new.set('STORAGE SQL', 'db', '')
        new.add_section('FILES')
        new.set('size_limit', 'ip', '')
        new.set('check_time', 'ip', '')
        new.add_section('OLD FILES')
        new.set('OLD FILES', 'archive', 'True')
        new.set('OLD FILES', 'processes', '1')
        new.set('OLD FILES', 'path', 'ftp/old/')
        new.set('OLD FILES', 'overload', 'True')
        new.set('OLD FILES', 'overload_threshold', '100')
        new.set('OLD FILES', 'log_level', 'INFO')
        new.add_section('MOBILE FILES')
        new.set('MOBILE FILES', 'archive', 'True')
        new.set('MOBILE FILES', 'processes', '1')
        new.set('MOBILE FILES', 'path', 'ftp/mobile/')
        new.set('MOBILE FILES', 'overload', 'False')
        new.set('MOBILE FILES', 'overload_threshold', '50')
        new.set('OLMOBILED FILES', 'log_level', 'INFO')

        with open('config.ini', 'w') as config_file:
            new.write(config_file)