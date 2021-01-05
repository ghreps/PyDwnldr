import logging
import multiprocessing as MP
import multiprocessing.managers as Manager

from time import sleep
from datetime import datetime

from models.ftp import Ftp
from models.files import Files
from models.config import Config
from models.databases import Database

class Old:

    config = Config()

    ARCHIVE = config.get('OLD FILES', 'archive')
    PROCESSES = int(config.get('OLD FILES', 'processes'))
    ROOT_PATH = config.get('OLD FILES', 'path')

    def __init__(self, filelist, scanners):
        self.filelist = filelist
        self.scanners = scanners
        
        # Get current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.path = self.ROOT_PATH + current_date

        self.main()

    def main(self):
        manager = Manager.SyncManager()
        manager.start()
        lock = manager.Lock()
        queue = manager.Queue()
        ftp = Ftp(self.path)
        processes = [None for foo in range(self.PROCESSES + 1)]
        # Start download files process
        processes[0] = MP.Process(target=ftp.download_files, args=(
                    self.filelist,
                    queue,
                    lock,
                    self.PROCESSES
                ))
        processes[0].start()
        # Start filtering files process
        for i in range(1, self.PROCESSES + 1):
            processes[i] = MP.Process(target=self.run, args=(
                    i,
                    queue,
                    lock
                ))
            processes[i].start()
        for i in range(len(processes)):
            processes[i].join()
      
    def run(self, process, queue, lock):
        print('[OLD FILES] Process ' + str(process) + ' start')
        sql_rows = set()
        files = Files(self.path)
        while True:
            lock.acquire()
            if not queue.empty():
                parts = queue.get()
                queue.task_done()
                lock.release()
                file = parts.split('|')
                if file[0] == 'DONE':
                    break
                # Clear from random macs
                try:
                    files.clear(file[1], True)
                except ValueError as e:
                    print('[OLD FILES] Cant clear random macs in {0}:\n{1}'.format(
                            file[1], e
                        ))
                    continue
                if self.ARCHIVE:
                    # Zip original file
                    files.zip('/original/', file[1])
                # Preparing sql
                for i, device in enumerate(self.scanners):
                    if file[1][0:10] == device['device_name']:
                        sql_rows.add(
                            '(NULL, "{0}","{1}","{2}","{3}"),'.format(
                                file[1],
                                file[3],
                                self.scanners[i]['customer_id'],
                                int(file[2])//1024
                        ))
                # ftp.connection.delete(file)
            else:
                lock.release()
            sleep(1)
        #connection = Database()
        sql = 'insert into `old_files` values '
        sql += ''.join(sql_rows)
        # if not connection.execute(sql[:-1] + ';'):
        #     ##Заливка не удалась, надо шот делать##
        #     print('err')
        print('[OLD FILES] Process ' + str(process) + ' end')