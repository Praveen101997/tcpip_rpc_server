from xmlrpc.server import SimpleXMLRPCServer
import base64
from config import name_server_info
import mysql.connector
from mysql.connector import Error

def init_user_table():
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS USERS (
            USERID INT AUTO_INCREMENT, 
            USERNAME TEXT NOT NULL, 
            PASSWORD TEXT NOT NULL, 
            SALT TEXT NOT NULL,
            PRIMARY KEY(USERID)
          );'''
    )
    cursor.execute(
        '''ALTER TABLE USERS
            ADD UNIQUE (USERNAME(20));'''
    )
    cursor.execute(
        '''ALTER TABLE USERS CHANGE USERID USERID INT(11) NOT NULL AUTO_INCREMENT;'''
    )

def init_server_table():
    connection.commit()
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS SERVERS (
            SERVERID INTEGER PRIMARY KEY, 
            ADDRESS TEXT NOT NULL
          );'''
    )
    cursor.execute(
        '''ALTER TABLE SERVERS CHANGE SERVERID SERVERID INT(11) NOT NULL AUTO_INCREMENT;'''
    )


def init_file_table():
    connection.commit()
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS FILES (
            FILEID INTEGER PRIMARY KEY,
            USERID INTEGER NOT NULL,
            SERVERID INTEGER NOT NULL,
            PATH TEXT NOT NULL,
            FILENAME TEXT NOT NULL,
            ISBACKUP INTEGER NOT NULL,
            FILEHASH TEXT NOT NULL,
            LASTMODIFIED INTEGER NOT NULL,
            FOREIGN KEY (USERID) REFERENCES USERS(USERID),
            FOREIGN KEY (SERVERID) REFERENCES SERVERS(SERVERID)
          );'''
    )
    cursor.execute(
        '''ALTER TABLE FILES CHANGE FILEID FILEID INT(11) NOT NULL AUTO_INCREMENT;'''
    )


def init_db():
    cursor.execute('DROP TABLE IF EXISTS FILES;')
    connection.commit()
    cursor.execute('DROP TABLE IF EXISTS SERVERS;')
    connection.commit()
    init_user_table()
    init_server_table()
    init_file_table()
    connection.commit()


def get_next_server():
    global server_counter

    try:
        cursor.execute('SELECT COUNT(*) FROM SERVERS;')
        result = cursor.fetchone()

        server_counter = (server_counter + 1) % result[0]

        cursor.execute('SELECT ADDRESS FROM SERVERS WHERE SERVERID = %s;', (server_counter + 1, ))
        address = cursor.fetchone()

        return address[0]
    except Error:
        return ''


def save_user(username, hash_password, salt):
    try:
        cursor.execute('INSERT INTO USERS (USERNAME, PASSWORD, SALT) VALUES (%s, %s, %s);',
                       (username, str(base64.b64decode(hash_password), 'utf-8'), str(base64.b64decode(salt), 'utf-8')))
        connection.commit()

        return True
    except Error:
        return False


def get_user_credentials(username):
    try:
        cursor.execute('SELECT USERID, PASSWORD, SALT FROM USERS WHERE USERNAME = %s;', (username, ))
        results = cursor.fetchone()

        return results
    except Error:
        return None


def get_server_addresses(user_id):
    try:
        cursor.execute('''SELECT DISTINCT ADDRESS FROM FILES JOIN SERVERS USING (SERVERID) 
                            WHERE ISBACKUP = 0 AND USERID = %s;''', (user_id, ))
        results = cursor.fetchall()

        return [address for (address, ) in results]
    except Error:
        return []


def register_file_server(server_id, address):
    try:
        cursor.execute('INSERT INTO SERVERS (SERVERID, ADDRESS) VALUES (%s, %s);', (server_id, address))
        connection.commit()

        return True
    except Error:
        return False


def unregister_file_server(server_id):
    cursor.execute('DELETE FROM FILES WHERE SERVERID = %s;', (server_id, ))
    cursor.execute('DELETE FROM SERVERS WHERE SERVERID = %s;', (server_id, ))
    connection.commit()


def save_file_info(file_list):
    try:
        cursor.executemany('''INSERT INTO FILES (USERID, SERVERID, PATH, FILENAME, ISBACKUP, FILEHASH, LASTMODIFIED) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s)''', file_list)
        connection.commit()

        return True
    except Error:
        return False


def get_file_infos(user_id, cloud_dir_paths):
    try:
        all_results = []

        for dir_path in cloud_dir_paths:
            cursor.execute('''SELECT FILENAME, LASTMODIFIED FROM FILES 
                                WHERE ISBACKUP = 0 AND USERID = %s AND PATH LIKE %s;''', (user_id, dir_path + '%'))
            all_results += cursor.fetchall()

        return all_results
    except Error:
        return []


def get_file_backup_servers(server_id, user_id, cloud_file_rel_path):
    try:
        cursor.execute('''SELECT ADDRESS FROM FILES JOIN SERVERS USING (SERVERID) 
                            WHERE ISBACKUP = 1 AND SERVERID != %s AND USERID = %s AND PATH = %s''',
                       (server_id, user_id, cloud_file_rel_path))
        results = cursor.fetchall()

        return [address for (address, ) in results]
    except Error:
        return []


def remove_file(user_id, cloud_file_rel_path):
    try:
        cursor.execute('DELETE FROM FILES WHERE USERID = %s AND PATH = %s;', (user_id, cloud_file_rel_path))
        connection.commit()

        return True
    except Error:
        return False


def get_file_hashes(user_id, cloud_file_rel_path):
    try:
        cursor.execute('''SELECT ISBACKUP, FILEHASH, ADDRESS FROM FILES JOIN SERVERS USING (SERVERID) 
                            WHERE USERID = %s AND PATH = %s ORDER BY ISBACKUP DESC;''', (user_id, cloud_file_rel_path))
        results = cursor.fetchall()

        return [(file_hash, address) for _, file_hash, address in results]
    except Error:
        return []


if __name__ == '__main__':
    server_counter = 0

    print('Processing...')

    connection = connection = mysql.connector.connect(host='remotemysql.com',port=3306,user='gYvrnIeX6r', passwd='nLC8k5w858', db='gYvrnIeX6r' )
    cursor = connection.cursor()

    init_db()

    with SimpleXMLRPCServer(name_server_info, allow_none=True) as server:
        server.register_function(get_next_server)
        server.register_function(save_user)
        server.register_function(get_user_credentials)
        server.register_function(get_server_addresses)
        server.register_function(register_file_server)
        server.register_function(unregister_file_server)
        server.register_function(save_file_info)
        server.register_function(get_file_infos)
        server.register_function(get_file_backup_servers)
        server.register_function(remove_file)
        server.register_function(get_file_hashes)

        try:
            print('Completed')
            print('For closing press ctrl^c')
            server.serve_forever()
        except KeyboardInterrupt:
            connection.close()
