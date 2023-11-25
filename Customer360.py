from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow import settings
from airflow.models import Connection

dag=DAG(
	dag_id='customer_360_pipeline',
	start_date=days_ago(1)
)

sensor=HttpSensor(
	task_id='watch_for_orders',
	http_conn_id='order_s3',
	endpoint='orders.csv',
	response_check=lambda response: response.status_code == 200,
	dag=dag,
	retry_delay=timedelta(minutes=5),
	retries=12
)

def get_url():
	session=settings.Session()
	connection=session.query(Connection).filter(Connection.conn_id == 'order_s3').first()
	return f'{connection.schema}://{connection.host}/orders.csv'

download_to_edgenode = BashOperator(
    task_id='download_orders',
    bash_command=f'ssh -i /home/cloudera/.ssh/id_rsa cloudera@10.0.2.15 "rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_url()} "',
    dag=dag
)


upload_order_info = BashOperator(
    task_id='upload_orders_to_hdfs',
    bash_command=f'ssh -i /home/cloudera/.ssh/id_rsa cloudera@10.0.2.15 "hdfs dfs -rm -R airflow_input && hdfs dfs -mkdir -p airflow_input && hdfs dfs -put ./airflow_pipeline/orders.csv airflow_input/"',
    dag=dag
)

def get_order_filter_cmd():
	cmd_zero='export SPARK MAJOR_VERSION=2'
	cmd_one='hdfs dfs -rm -R -f airflow_output'
	cmd_two= 'spark-submit --class Customer airflow_input/orders.csv airflow_output'
	return f'{cmd_zero} && {cmd_one} && {cmd_two}'

process_order_info = BashOperator(
    task_id='process_orders',
    bash_command=f'ssh -i /home/cloudera/.ssh/id_rsa cloudera@10.0.2.15 "get_order_filter_cmd()"',
    dag=dag
)

def create_order_hive_table_cmd():
	return 'hive -e "CREATE external table if not exists airflow.orders(order_id int, order_date string, customer_id int, status string) row format delimited fields terminated by \',\' stored as textfile location \'/user/cloudera/ariflow_output\'"'

create_order_table = BashOperator(
    task_id='create_order_table_hive',
    bash_command=f'ssh -i /home/cloudera/.ssh/id_rsa cloudera@10.0.2.15 "create_order_hive_table_cmd()"',
    dag=dag
)

def load_hbase_cmd():
	cmd_one = (
    'hive -e "create table if not exists airflow_hbase('
    'customer_id int, customer_fname string, customer_lname string, '
    'order_id int, order_date string) STORED BY '
    '\'org.apache.hadoop.hive.HBaseStorageHandler\' with '
    'SERDEPROPERTIES(\'hbase.columns.mapping\'=\':key,personal:customer_fname, '
    'personal:customer_lname, personal:order_id, personal:order_date\')"'
)
	cmd_two='inset overwrite table airflow.airflow_hbase select c.customer_id, c.customer_fname, c.customer_lname,o.order_id, o.order_date from airflow.customers c join airflow.orders o '
	return f'{cmd_one} && {cmd_two}'

load_hbase = BashOperator(
    task_id='load_hbase_table',
    bash_command=f'ssh -i /home/cloudera/.ssh/id_rsa cloudera@10.0.2.15 "load_hbase_cmd()"',
    dag=dag
)

sensor >> download_to_edgenode >> upload_order_info >> process_order_info >> create_order_table >> load_hbase 

