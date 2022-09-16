from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient
import json
import logging
import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

task_logger = logging.getLogger('airflow.task')

pg_dwh = 'PG_WAREHOUSE_CONNECTION'

# class for Mongo
class MongoConnect:
    def __init__(self,
                 cert_path: str,
                 user: str,
                 pw: str,
                 hosts: str,
                 rs: str,
                 auth_db: str,
                 main_db: str
                 ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = hosts
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    # Формируем строку подключения к MongoDB
    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=self.hosts,
            rs=self.replica_set,
            auth_src=self.auth_db)

    # Создаём клиент к БД
    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]

cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
rs = Variable.get("MONGO_DB_REPLICA_SET")
db = Variable.get("MONGO_DB_DATABASE_NAME")
hosts = Variable.get("MONGO_DB_HOST")

mongo_connect = MongoConnect(cert_path, db_user, db_pw, hosts, rs, db, db)

def get_docs_from_mongo(pg_schema, pg_table_check, collection, pg_table_dwh):

    # connect to dwh
    dwh_hook = PostgresHook(pg_dwh)
    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    # connect to mongo
    mongo_conn = mongo_connect.client()

    # filters to get data from mongo
    # datetime of last update
    old_max_update = dwh_hook.get_first(f"SELECT max(update_ts) FROM {pg_schema}.{pg_table_check} WHERE table_name = '{collection}'")[0]

    if old_max_update is None:
        old_max_update = datetime(1900, 1, 1)

    task_logger.info(f"old_max_update is {old_max_update}")

    filter = {'update_ts': {'$gt': old_max_update}}

    sort = [('update_ts', 1)]
    
    # get data from mongo
    docs = list(mongo_conn.get_collection(f"{collection}").find(filter=filter, sort=sort))
    
    # load data to dwh

    sql = f'''
        INSERT INTO {pg_schema}.{pg_table_dwh} (object_id, object_value, update_ts)
        VALUES(%s, %s, %s)
        ON CONFLICT (object_id) DO UPDATE
        SET
        object_value = EXCLUDED.object_value,
        update_ts = EXCLUDED.update_ts
        '''

    for i in docs:
        cursor.execute(sql, (f"{i['_id']}", f"{json.dumps(i, default=str)}", f"{i['update_ts']}"))
    conn.commit() 

    task_logger.info(f"{len(docs)} rows upload to {collection}")
    

    # get new datetime of last update
    new_max_update = dwh_hook.get_first(f"select max(update_ts) from {pg_schema}.{pg_table_dwh}")[0]

    task_logger.info(f"new_max_update is {new_max_update}")

    # load new_max_update to mongo_etl_settings
    if new_max_update > old_max_update:
        cursor.execute(f"INSERT INTO {pg_schema}.{pg_table_check} (update_ts, table_name) VALUES(%s, %s)",
                            (f"{new_max_update}", f"{collection}"))
        conn.commit() 

        task_logger.info(f"new max update load to mongo_etl_settings for table {collection}")
        
    cursor.close()
    conn.close()


# params for dag
default_args = {
    'owner':'maks',
    'retries':1,
    'retry_delay': timedelta (seconds = 60)
}

local_tz = pendulum.timezone("Europe/Moscow")

# check before prod

dag = DAG('01_dag_upload_stage_from_mongo',
        start_date=pendulum.datetime(2022, 8, 27, tz=local_tz),
        catchup=True,
        schedule_interval='5 0 * * *',
        max_active_runs=1,
        default_args=default_args)


mongo_restaraunts = PythonOperator(
            task_id = 'mongo_restaraunts',
            python_callable = get_docs_from_mongo,
            op_kwargs = {
                'pg_schema' : 'prod_dv_stg',
                'pg_table_check' : 'mongo_etl_settings',
                'collection' : 'restaurants',
                'pg_table_dwh' : 'order_system_restaurants'
            },
            dag = dag
)


mongo_users = PythonOperator(
            task_id = 'mongo_users',
            python_callable = get_docs_from_mongo,
            op_kwargs = {
                'pg_schema' : 'prod_dv_stg',
                'pg_table_check' : 'mongo_etl_settings',
                'collection' : 'users',
                'pg_table_dwh' : 'order_system_users'
            },
            dag = dag
)


mongo_orders = PythonOperator(
            task_id = 'mongo_orders',
            python_callable = get_docs_from_mongo,
            op_kwargs = {
                'pg_schema' : 'prod_dv_stg',
                'pg_table_check' : 'mongo_etl_settings',
                'collection' : 'orders',
                'pg_table_dwh' : 'order_system_orders'
            },
            dag = dag
)
[mongo_orders, mongo_restaraunts, mongo_users]
