import pymysql

from models.config import Config

class Database:
    cabinet = None
    storage = None

    def __init__(self):
        config = Config()

        self.check()

    def connect(self, is_cabinet = False):
        if is_cabinet:
            try:
                self.cabinet = pymysql.connect(
                    host='127.0.0.1',
                    user='root',
                    port=3306,
                    password='hardpass',                             
                    db='cabinet',
                    cursorclass=pymysql.cursors.DictCursor
                )
                self.cabinet.autocommit(True)
            except pymysql.Error as e:
                print('[ERROR][DATABASE] Cabinet database init fail {0}: {1} '.format(
                    e.args[0],
                    e.args[1]
                ))
                self.check()
        else:
            try:
                self.storage = pymysql.connect(
                    host='127.0.0.1',
                    user='root',
                    port=3307,
                    password='hardpass',                             
                    db='storage',
                    cursorclass=pymysql.cursors.DictCursor
                )
                self.storage.autocommit(True)
            except pymysql.Error as e:
                print('[ERROR][DATABASE] Storage database init fail {0}: {1} '.format(
                    e.args[0],
                    e.args[1]
                ))
                self.check()

    def check(self):
        if self.cabinet is None or (self.cabinet is not None and self.cabinet.open is False):
            self.connect(True)
        # if self.storage is None or (self.storage is not None and self.storage.open is False):
        #     self.connect()

    def query(self, sql, is_cabinet = False):
        try:
            cursor = None
            if is_cabinet:
                self.cabinet.ping(reconnect=True)
                cursor = self.cabinet.cursor()
            else:
                self.storage.ping(reconnect=True)
                cursor = self.storage.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            print(str(e))
            if is_cabinet:
                self.cabinet.rollback()
            else:
                self.storage.rollback()
            return False
    
    def execute(self, sql, is_cabinet = False):
        try:
            cursor = None
            if is_cabinet:
                self.cabinet.ping(reconnect=True)
                cursor = self.cabinet.cursor()
            else:
                self.storage.ping(reconnect=True)
                cursor = self.storage.cursor()
            cursor.execute(sql)
            cursor.close()
            return True
        except Exception as e:
            print(str(e))
            if is_cabinet:
                self.cabinet.rollback()
            else:
                self.storage.rollback()
            return False

    def get_linked_scanners(self):
        sql = (
            'select c.id, s.device_name, s.keys_type_id, f.`customer_id` from customer c '
            'inner join `facility` f on f.`customer_id` = c.`id` '
            'inner join `device` d on d.`facility_id` = f.`id` '
            'inner join `keys` s on s.`key` = d.`key`'
        )
        rows = self.query(sql, True)
        if rows:
            return tuple(rows)
        else:
            print('Please check database config and restart')
            input()

    def get_gof_scanners(self):
        sql = 'select device_name, has_key_id, customer_id from `keys` where `keys_type_id` = 10'
        rows = self.query(sql, True)
        if rows:
            return tuple(rows)
        else:
            print('Please check database config and restart')
            input()