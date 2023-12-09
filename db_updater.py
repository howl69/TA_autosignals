from moexalgo.metrics import prepare_request, pandas_frame
from clickhouse_driver import Client
import datetime as dt
import logging
import time
from vars import HOST

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("service started")

def tradestats(date = None, latest = None,
                   offset = None, limit = None, cs = None):
    
    metrics_it = prepare_request('tradestats', cs, from_date=date, latest=latest,
                                            offset=offset, limit=(limit or 25000))
    return pandas_frame(metrics_it)
        


def main():
    client = Client(HOST, settings={"use_numpy": True})
    while(True):
        latest_data = tradestats(latest=True)
        
        latest_data = latest_data.rename(columns = {'ticker':'secid', 'systime':'SYSTIME'})
        latest_date, latest_time = latest_data.loc[0,['tradedate', 'tradetime']].values

        db_latest_date, db_latest_time = client.execute(
            """
                SELECT tradedate, tradetime 
                FROM algopack 
                ORDER BY tradedate DESC, tradetime DESC 
                LIMIT 1
            """)[0]
        
        insert_flag = False
        if db_latest_date < latest_date:
            insert_flag = True
        elif db_latest_date == latest_date:
            db_latest_time = dt.datetime.strptime(db_latest_time, '%H:%M:%S').time()
            if db_latest_time < latest_time:
                insert_flag = True
        else:
            raise Exception(f'Future data in DB?{db_latest_date}, {latest_date}')

        if insert_flag:
            latest_data['tradetime'] = latest_data['tradetime'].apply(lambda x: x.strftime("%H:%M:%S"))
            latest_data['tradedate'] = latest_data['tradedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
            latest_data['SYSTIME'] = latest_data['SYSTIME'].apply(lambda x: x.strftime("%Y-%m-%d, %H:%M:%S"))
            
            client.insert_dataframe('INSERT INTO algopack VALUES', latest_data)
            logger.info("INSERTED")
        logger.info('sleep')
        time.sleep(10)

if __name__ == '__main__':
    main()