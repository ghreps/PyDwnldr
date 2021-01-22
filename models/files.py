import os
import re
import zipfile

import pandas
import numpy

import asyncio
import aiohttp

from platform import system
from time import time

from models.config import Config
from models.telegram import tg_send_msg

class Files:

    config = Config()
    macs_list = tuple()
    URL = 'https://linuxnet.ca/ieee/oui/nmap-mac-prefixes'

    def __init__(self, path = ''):
        if path:
            self.path = path
            self.read_macs()
    
    async def check_mac_file(self):
        try:
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
                    try:
                        await self.mac_update()
                    except Exception as e:
                        await tg_send_msg('[CHECK MAC FILE] Cant update macs.txt. Skipping...\n\n' + str(e))                
                else:
                    self.read_macs()
            else:
                try:
                    await self.mac_update()
                except Exception as e:
                    raise Exception('Cant download macs.txt. Retry...\n\n' + str(e))
        except Exception as e:
            await tg_send_msg('[CHECK MAC FILE] ' + str(e))
            await asyncio.sleep(60)
            await self.check_mac_file()

    def read_macs(self):
        temp_list = set()
        with open('macs.txt', 'r') as file:
            for line in file: 
                row = re.sub(r'\t.*\n', '', line).lower()
                temp_list.add(row[0:2] + ':' + row[2:4] + ':' + row[4:6])
        self.macs_list = tuple(temp_list)

    async def mac_update(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.URL, headers={'User-Agent': 'PostmanRuntime/7.26.8'}) as response:
                    request = await response.read()
        except Exception as e:
            raise e
        else:
            if os.path.exists('macs.txt'):
                os.remove('macs.txt')
            file = open('macs.txt','wb')
            file.write(request)
            file.close()
            self.read_macs()

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
            raise file_name
        else:
            if delete:
                os.remove(file_path)
            return True