import pymysql
from config import *

connection = pymysql.connect(
    host = host_config,
    user = user_config,
    password = password_config,
    db = db_config,
    charset = 'utf8',
    cursorclass = pymysql.cursors.DictCursor)

cursor = connection.cursor()