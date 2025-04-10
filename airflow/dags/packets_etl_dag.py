from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

base_dir = '/home/dyab/projects/PacketX-Lakehouse'
default_args = {
    'owner': 'you',
    'start_date': datetime(2025, 4, 10),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'packetx_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run PacketX ETL scripts sequentially',
) as dag:
    
    test_server_connection = SSHOperator(
        task_id='test_server_connection',
        command=f'echo connected to server successfully',
        ssh_conn_id='packetX_server',
        dag=dag,
    )
    
    ingest_source_data = SSHOperator(
        task_id='ingest_source_data',
        command=f'cd {base_dir} && source .venv/bin/activate && python3 upload_to_s3.py',
        ssh_conn_id='packetX_server',
        dag=dag,
    )

    test_wh_etl_before = SSHOperator(
        task_id='test_wh_etl_before',
        command=f'cd {base_dir} && source .venv/bin/activate && python3 test_wh_etl.py',
        ssh_conn_id='packetX_server',
        dag=dag,
    )

    raw_s3_iceberg_etl = SSHOperator(
        task_id='raw_s3_iceberg_etl',
        command=f'cd {base_dir} && source .venv/bin/activate && python3 raw_s3_iceberg_etl.py',
        ssh_conn_id='packetX_server',
        dag=dag,
    )

    warehouse_etl = SSHOperator(
        task_id='warehouse_etl',
        command=f'cd {base_dir} && source .venv/bin/activate && python3 warehouse_etl.py',
        ssh_conn_id='packetX_server',
        dag=dag,
    )

    test_wh_etl_after = SSHOperator(
        task_id='test_wh_etl_after',
        command=f'cd {base_dir} && source .venv/bin/activate && python3 test_wh_etl.py',
        ssh_conn_id='packetX_server',
        dag=dag,
    )

    dynamodb_etl = SSHOperator(
        task_id='dynamodb_etl',
        command=f'cd {base_dir} && source .venv/bin/activate && python3 dynamodb_etl.py',
        ssh_conn_id='packetX_server',
        dag=dag,
    )



    # Task sequence
    test_server_connection >> ingest_source_data >> test_wh_etl_before
    test_wh_etl_before >> raw_s3_iceberg_etl >> warehouse_etl
    warehouse_etl >> test_wh_etl_after >> dynamodb_etl