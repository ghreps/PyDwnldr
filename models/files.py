import os
import re
import zipfile

import pandas
import numpy

from requests import get, exceptions
from platform import system
from time import time

from models.config import Config

class Files:

    config = Config()
    macs_list = tuple()
    URL = 'https://linuxnet.ca/ieee/oui/nmap-mac-prefixes'

    def __init__(self, path = ''):
        if path:
            self.path = path
            self.read_macs()
    
    def check_mac_file(self):
        if os.path.exists('macs.txt'):
            now = time()
            lifetime = self.config.get('MACS LIST', 'lifetime')
            if system() == 'Windows':
                file_date = os.path.getmtime('macs.txt')
            else:
                stat = os.stat('macs.txt')
                try:
                    file_date = stat.st_birthtime
                except AttributeError:
                    file_date = stat.st_mtime
            if abs((now - file_date))//86400 > float(lifetime):
                if not self.mac_update():
                    print('[MAC FILE] Cant update .txt. Skipping...')
            else:
                self.read_macs()
        else:
            if not self.mac_update():
                input()
    
    def read_macs(self):
        temp_list = set()
        with open('macs.txt', 'r') as file:
            for line in file: 
                row = re.sub(r'\t.*\n', '', line).lower()
                temp_list.add(row[0:2] + ':' + row[2:4] + ':' + row[4:6])
        self.macs_list = tuple(temp_list)

    def mac_update(self):
        try:
            request = get(self.URL, headers={'User-Agent': 'PostmanRuntime/7.26.8'})
        except exceptions.RequestException as e:
            print('[MAC UPDATE] Cant download .txt. Stopping...\n' + e)
            return False
        else:
            if os.path.exists('macs.txt'):
                os.remove('macs.txt')
            file = open('macs.txt','wb')
            file.write(request.content)
            file.close()
            self.read_macs()
            return True

    def is_duplicate(self, file, datetime):
        path = '{0}{1}{2}'.format(
                self.path,
                '/original/',
                file[:-3]
            )
        if os.path.exists(path + 'txt'):
            return '{0}_{1}.txt'.format(
                file[:-4],
                datetime[11:].replace(':', '-')
            )
        elif os.path.exists(path + 'zip'):
            return '{0}_{1}.txt'.format(
                file[:-4],
                datetime[11:].replace(':', '-')
            )
        else:
            return False

    def clear(self, file_name, zip = False):
        #print('[CLEAR] File start ' + file_name)
        try:
            new_file = '{0}/filtered/{1}'.format(self.path, file_name)
            old_file = '{0}/original/{1}'.format(self.path, file_name)
            df = pandas.read_csv(old_file,
                sep = ";",
                header = None,
                names = ['id', 'device', 'mac', 'date', 'time', 'signal']
            )
            df2 = df.dropna()
            # Слишком длинно, пока не знаю как сократить не создавая доп. переменные
            df2[df2.mac.str[0:8].isin(self.macs_list)].groupby(['mac', 'date', 'time'])['signal'].mean().round(0).astype(int).reset_index().sort_values(by=['date', 'time']).to_csv(new_file, header=None, index=None, sep=';', mode='w')
            # Zip filtered file
            if zip:
                self.zip('/filtered/', file_name)
            #print('[CLEAR] File end ' + file_name)
        except Exception as e:
            raise ValueError(e)

    def zip(self, folder, file_name, delete = True):
        file_path = self.path + folder + file_name
        try:
            with zipfile.ZipFile(
                    file_path + '.zip', 'w', zipfile.ZIP_DEFLATED
                ) as z:
                z.write(file_path, file_name)
        except zipfile.BadZipFile:
            print('[ZIP FILE] Error to zip file ' + file_name)
            return False
        else:
            if delete:
                os.remove(file_path)
            return True