import logging
from logging.handlers import QueueListener, QueueHandler

import multiprocessing as MP
import multiprocessing.managers as Manager

import asyncio

from time import sleep
from datetime import datetime

from models.ftp import Ftp
from models.files import Files
from models.config import Config
from models.databases import Database
from models.telegram import tg_send_msg

class Old:

    config = Config()

    ARCHIVE = config.get('OLD FILES', 'archive')
    PROCESSES = int(config.get('OLD FILES', 'processes'))
    ROOT_PATH = config.get('OLD FILES', 'path')

    formatter = '[%(asctime)s][%(levelname)s][%(name)s] %(message)s'
    logging.basicConfig(level=logging.INFO)

    def __init__(self, filelist, scanners):
        self.filelist = filelist
        self.scanners = scanners
        
        # Get current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.path = self.ROOT_PATH + current_date

        self.main()

    def main(self):
        # Log
        log_queue = MP.Queue(-1)
        file_handler = logging.FileHandler("logs/downloads/old.log")
        file_handler.setFormatter(logging.Formatter(self.formatter))
        queue_listener = QueueListener(log_queue, file_handler)
        queue_listener.start()
        #
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
                    str(i),
                    queue,
                    lock,
                    log_queue
                ))
            processes[i].start()
        for i in range(len(processes)):
            processes[i].join()
        queue_listener.stop()
      
    def run(self, process, queue, lock, log_queue):
        log = '[Process ' + process + '] '
        log_name = 'app.downloads.old\n'
        logger = logging.getLogger(log_name)
        logger.addHandler(QueueHandler(log_queue))
        logger.info(log + 'Start process')
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
                logger.debug('{0}Start {1}'.format(log, file[1]))
                # Clear from random macs
                try:
                    files.clear(file[1], True)
                except Exception as e:
                    error = '{0}Cant clear macs in {1}: {2}'.format(
                        log, file[1], e
                    )
                    logger.error(error)
                    asyncio.run(tg_send_msg(log_name + error))
                    continue
                if self.ARCHIVE:
                    # Zip original file
                    try:
                        files.zip('/original/', file[1])
                    except Exception as e:
                        error = '{0}Cant zip {1}'.format(log, e)
                        logger.warn(error)
                        asyncio.run(tg_send_msg(log_name + error))
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
                try:
                    pass
                    #Ftp().delete_file(file[0])
                except Exception as e:
                    error = '{0}Cant delete {1} from ftp: {2}'.format(log, file[0], e)
                    logger.warn(error)
                    asyncio.run(tg_send_msg(log_name + error))
                logger.debug('[Process' + process + '] End ' + file[1])
            else:
                lock.release()
            sleep(1)
        #connection = Database()
        sql = 'insert into `old_files` values '
        sql += ''.join(sql_rows)
        # if not connection.execute(sql[:-1] + ';'):
        #     ##Заливка не удалась, надо шот делать##
        #     print('err')
        logger.info('[Process ' + process + '] End process')
