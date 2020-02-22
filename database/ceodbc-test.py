import logging
import datetime
import sys
import ceODBC


logging.basicConfig( level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s : %(name)s - %(levelname)s - %(filename)s : %(lineno)d -- %(message)s")

conn = None
host = '127.0.0.1'
db = 'Mapper'
user = 'sa'
password='t6r5e4w3q2'


def init_connection():
    '''
    init connection with settings in ENV, test connection by get server version.
    :return:
    '''

    d1=datetime.datetime.now()
    logging.info(d1)
    # init connection, test by get version.
    global conn
    conn = ceODBC.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+host+';DATABASE='+db+';UID='+user+';PWD='+ password)

    d2=datetime.datetime.now()
    logging.info('%s,%s',d2,d2-d1)

    check_connection()

def check_connection():
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("select @@version as version")
        version = cursor.fetchone()[0]

        logging.info("Connect to %s, db %s with user %s" % (host, db, user))
        logging.info("SQL Server version %s", version)
        return True
    except Exception as e:
        logging.error("check connection failed. %s" % (e))
        return False

def test_insert_data():
    if not conn:
        return False

    countSql = "select count(1) from odb.fab_item"
    sql = "select * from odb.fab_item  order by fab_item_id offset ? row fetch next ? row only"

    countCursor = conn.curson()
    countCursor.execute(countSql)
    total = cursor.fetchone()
    logging.info('total:')
    logging.info(total)


if __name__=='__main__' :
    test_insert_data()
    conn.close()
