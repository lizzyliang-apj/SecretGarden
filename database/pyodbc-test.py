import logging
import datetime
import sys
import pyodbc
import math

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
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+host+';DATABASE='+db+';UID='+user+';PWD='+ password)

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

        # logging.info("Connect to %s, db %s with user %s" % (host, db, user))
        # logging.info("SQL Server version %s", version)
        return True
    except Exception as e:
        logging.error("check connection failed. %s" % (e))
        return False

pagesize = 10000

def getTotalPage():
    if not check_connection():
        init_connection()
    countSql = "select count(1) from fab_item"
    cursor = conn.cursor()
    cursor.execute(countSql)
    total = cursor.fetchone()[0]

    totalPage = math.ceil(total / pagesize)

    logging.info('total:%s,page:%s,totalPage:%s:',total,pagesize,totalPage)
    logging.info('total:%s',total)

    return totalPage

def getDataByPage(pageIndex):
     if not check_connection():
        init_connection()

     sql = (
        "SELECT FAB_ITEM_ID,COMBO_NAME,FCR_NO,FCR_COMBO_ID,GARMENT_WASH,GF_NO,GF_ID,LEAD_TIME_DAYS,STATUS,CREATE_USER_ID,"
        "CREATE_DATE,LAST_MODI_USER_ID,LAST_MODI_DATE,VERTICAL_REPEAT,HORIZONTAL_REPEAT,REPEAT_REMARK,FABRIC_WIDTH,CONSTRUCTION,"
        "FINISHING,SHRINKAGE,FABRIC_ID,FABRIC_CODE,FABRIC_CODE_TYPE,FABRIC_NATURE_CD,REPEAT_UOM,MILL_REF_NO,FABRIC_TYPE,PATTERN,"
        "COMPONENT,WEFT_REPEAT,WARP_REPEAT,WEFT_WARP_REASON,WEFT_WARP_UNIT,GEW_DIGITAL_IMAGE_FLAG "
        " FROM FAB_ITEM  ORDER BY FAB_ITEM_ID OFFSET ? ROW FETCH NEXT ? ROW ONLY"
       )

     params = []
     params.append(pageIndex)
     params.append(pagesize)

     cursor = conn.cursor()
     cursor.execute(sql, tuple(params))

     return cursor


def test_insert_data():

    totalPage = getTotalPage()

    insertSql = "INSERT INTO FAB_ITEM_copy2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    currentPage = 0

    cursor = conn.cursor()
    begin = datetime.datetime.now()
    while currentPage < totalPage:
        logging.info('page %d',currentPage+1)

        get1 = datetime.datetime.now()
        page = getDataByPage(currentPage * pagesize)
        get2 = datetime.datetime.now()
        logging.info('select sql %s page begin at %s,end at %s, takes %s seconds',currentPage+1,get1,get2,get2-get1)

        params = []
        for row in page:
          params.append(row)
          # logging.info(params)

        insert1 = datetime.datetime.now()
        cursor.executemany(insertSql,params)
        conn.commit()
        insert2 = datetime.datetime.now()
        logging.info('insert to table page (%d) begin at %s,end at %s, takes %s seconds',currentPage+1,insert1,insert2,insert2-insert1)

        currentPage = currentPage +1

    end = datetime.datetime.now()
    logging.info('job begin at %s,end at %s, takes %s seconds',begin,end,end-begin)

if __name__=='__main__' :
    test_insert_data()
    if not conn:
      conn.close()
