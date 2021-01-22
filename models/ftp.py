from ftplib import FTP
import os 

from datetime import datetime

from time import sleep

from models.config import Config
from models.files import Files

class Ftp:

    def __init__(self, path = ''):
        self.config = Config()
        self.ip = self.config.get('FTP', 'host')
        self.dir = self.config.get('FTP', 'dir')
        self.user = self.config.get('FTP', 'user')
        self.pswd = self.config.get('FTP', 'pswd')
        self.path = path

        if path != '':
            # Create folder if not exists
            if not os.path.exists(path + '/original'):
                os.makedirs(path + '/original')
                os.makedirs(path + '/filtered')

    def get_files(self):
        try:
            with FTP(self.ip, self.user, self.pswd) as ftp:
                temp, files = [], []
                ftp.cwd(self.dir) 
                ftp.retrlines('LIST', temp.append)
                for line in temp:
                    parts = line.split()
                    if parts[-1][-4:] == '.txt':
                        files.append([parts[-1], parts[4]])
                print('[FTP]', len(files), '.txt files')
                return tuple(files)
        except Exception as e:
            raise e

    def download_files(self, filelist, queue, lock, processes):
        files = Files(self.path)
        for i in filelist:
            file = i.split(':')
            file_name = file[0]
            path_original = self.path + '/original/'
            current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if files.is_duplicate(file_name, current_datetime):
                file_name = files.is_duplicate(file_name, current_datetime)
            answer = ''
            try:
                with FTP(self.ip, self.user, self.pswd) as ftp:
                    ftp.cwd(self.dir)
                    with open(path_original + file_name, 'wb') as f:
                        answer = ftp.retrbinary('RETR ' + file[0], f.write)
            except Exception as e:
                raise ValueError(e)
            if not answer.startswith('226 Transfer complete'):
                if os.path.exists(path_original + file_name):
                    os.remove(path_original + file_name)
            else:
                lock.acquire()
                queue.put('{0}|{1}|{2}|{3}'.format(
                    file[0],
                    file_name,
                    file[1],
                    current_datetime
                ))
                lock.release()
        lock.acquire()
        for i in range(processes):
            queue.put('DONE')
        lock.release()
    
    def check_size(self, file, size):
        try:
            with FTP(self.ip, self.user, self.pswd) as ftp:
                ftp.cwd(self.dir)
                sleep(int(self.config.get('FILES', 'check_time')))
                new_size = ftp.size(file)
                if new_size != int(size):
                    return False
                else:
                    return True
        except Exception as e:
            raise ValueError(e)
    
    def delete_file(self, file):
        try:
            with FTP(self.ip, self.user, self.pswd) as ftp:
                ftp.delete(file)
        except Exception as e:
            raise ValueError(e)
            