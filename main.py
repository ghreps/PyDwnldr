from models.ftp import Ftp
from models.files import Files
from models.redis import Redis
from models.config import Config
from models.databases import Database
from models.download.old import Old
from models.download.mobile import Mobile

from multiprocessing import Process

CHECK_FILE_SIZE = False
DATE_THRESHOLD = 31

def get_linked_scanners():
    print('[get_scanners_info] Получаем список привязанных сканеров')
    connection = Database()
    sql = (
        'select c.id, s.device_name, s.keys_type_id, f.`customer_id` from customer c '
        'inner join `facility` f on f.`customer_id` = c.`id` '
        'inner join `device` d on d.`facility_id` = f.`id` '
        'inner join `keys` s on s.`key` = d.`key`'
    )
    rows = connection.query(sql, True)
    if rows:
        return rows
    else:
        print('Please check database config and restart')
        input()

def get_gof_scanners():
    connection = Database()
    sql = 'select device_name, has_key_id, customer_id from `keys` where `keys_type_id` = 10'
    rows = connection.query(sql, True)
    if rows:
        return rows
    else:
        print('Please check database config and restart')
        input()

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
    ftp = Ftp()
    config = Config()
    threads = [i for i in range(4)]
    threads[0] = int(config.get('SIMPLE FILES', 'processes'))
    threads[1] = int(config.get('MOBILE FILES', 'processes'))
    threads[2] = int(config.get('OLD FILES', 'processes'))
    threads[3] = int(config.get('GOF FILES', 'processes'))
    files = Files()
    files.check_mac_file()
    processes = [None for i in range(4)]
    file_lists = [[] for i in range(4)]
    # Get ftp file list
    ftp_files = ftp.get_files()
    if len(ftp_files) != 0:
        # Get scanner types 
        scanners = tuple(get_linked_scanners())
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
                        if not ftp.check_size(file[0], file[1]):
                            print('[MAIN] Файл ' + file[0] + ' не дозагружен')
                            continue
                    except Exception as e:
                        print('[MAIN] Ошибка проверки дозагрузки ' + e)
                        continue
                # Check for old date
                days_difference = files.day_difference(file[0][11:21])
                # if not mobile
                if len(file[0]) == 25:
                    if scanner_type != 3:
                        if days_difference <= DATE_THRESHOLD:
                            # simple files
                            file_lists[0].put(file[0] + ':' + file[1])
                        else: # old files
                            file_lists[2].append(file[0] + ':' + file[1])
                    else: # gof files
                        file_lists[3].put(file[0] + ':' + file[1])
                else: # mobile files
                    file_lists[1].put(file[0] + ':' + file[1])
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