from models.ftp import Ftp
from models.files import Files
#from models.redis import Redis
from models.config import Config
from models.telegram import tg_send_msg
import logger

import logging
import asyncio
import time



from models.databases import Database
from models.download.old import Old
from models.download.mobile import Mobile

from datetime import datetime
from multiprocessing import Process

CHECK_FILE_SIZE = False
DATE_THRESHOLD = 31

def start_processes(type, files, scanners):
    if type == 0:
        pass
    elif type == 1:
        Mobile(files, scanners)
    elif type == 2:
        Old(files, scanners)
    elif type == 3:
        pass

#
# 0 - simple files
# 1 - mobile files
# 2 - old files
# 3 - gof files
#

if __name__ == '__main__':
    processes = [None for i in range(4)]
    file_lists = [[] for i in range(4)]
    ftp_files = False
    asyncio.get_event_loop().run_until_complete(Files().check_mac_file())
    while not ftp_files:
    # Get ftp file list
        ftp_files = Ftp().get_files()
        if not ftp_files:
            time.sleep(300)
    if len(ftp_files) != 0:
        # Get scanner types 
        scanners = Database().get_linked_scanners()
        for file in ftp_files:
            # Check for availability
            scanner_type = -1
            try:
                scanner_type = scanners[[x['device_name'] for x in scanners].index(file[0][0:10])]['keys_type_id']
            except:
                pass
            if scanner_type != -1:
                # Check for full download
                if CHECK_FILE_SIZE:
                    try:
                        if not Ftp().check_size(file[0], file[1]):
                            print('[MAIN] Файл ' + file[0] + ' не дозагружен')
                            continue
                    except Exception as e:
                        print('[MAIN] Ошибка проверки дозагрузки ' + e)
                        continue
                # Check for old date
                given_date = datetime.strptime(file[0][11:21], '%Y-%m-%d')
                current_date = datetime.strptime(str(datetime.now())[0:10], '%Y-%m-%d')
                days_difference = abs((current_date - given_date).days)
                # if not mobile
                if len(file[0]) == 25:
                    if scanner_type != 3:
                        if days_difference <= DATE_THRESHOLD:
                            # simple files
                            file_lists[0].append(file[0] + ':' + file[1])
                        else: # old files
                            file_lists[2].append(file[0] + ':' + file[1])
                    else: # gof files
                        file_lists[3].append(file[0] + ':' + file[1])
                else: # mobile files
                    file_lists[1].append(file[0] + ':' + file[1])
            else:
                continue
        print('Start new processes...')
        for i in range(len(processes)):
            if len(file_lists[i]) != 0:
                processes[i] = Process(target=start_processes, args=(
                    i,
                    file_lists[i],
                    scanners
                ))
                processes[i].start()
    else:
        print('FTP is empty')
    # Close FTP
    # Wait for processes
    for i in range(len(processes)):
        if processes[i] != None:
            processes[i].join()
    # Close queues
    # for i in range(len(queue_list)):
    #     try:
    #         queue_list[i].close()
    #     except:
    #         pass
    # print('end')