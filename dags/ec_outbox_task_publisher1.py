from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pika
import psycopg2.extras
from psycopg2 import pool
import json
import logging

# Initialize logger
logger = logging.getLogger(__name__)

db_conn_pool = pool.SimpleConnectionPool(
    minconn = Variable.get('ec_db_min_conn'),  # Minimum number of connections in the pool
    maxconn = Variable.get('ec_db_max_conn'),  # Maximum number of connections in the pool
    host = Variable.get('ec_db_host'),
    port = Variable.get('ec_db_port'),
    dbname = Variable.get('ec_db_name'),
    user = Variable.get('ec_db_user'),
    password = Variable.get('ec_db_password')
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 27, 4, 50),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='ec_outbox_task_publisher_dag',
    default_args=default_args,
    catchup=False,
    # tags=['example'],
    schedule_interval=timedelta(minutes=5)
)
def publish_outbox_task():
    # @task
    # def print_config():
    #     logger.info("Fetching RabbitMQ configuration variables")
    #     host = Variable.get('ec_rabbitmq_host')
    #     username = Variable.get('ec_rabbitmq_username')
    #     password = Variable.get('ec_rabbitmq_password')
    #     port = Variable.get('ec_rabbitmq_port')
    #     logger.info(f"RabbitMQ Host: {host}, Port: {port}, Username: {username}")
    #     return (host, username, password, port)

    @task
    def read_outbox(schema):
        query = f"""
            SELECT * FROM {schema}.outbox WHERE is_processed = false;
        """

        try:
            conn = db_conn_pool.getconn()
            logger.info('Successfully connected to PostgreSQL database')

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()

            rows = []
            for row in results:
                msg = dict(row)
                payload_data = json.loads(msg["payload"]) if isinstance(msg["payload"], str) else msg["payload"]

                json_obj = {
                    "outboxId": msg["id"],
                    "eventType": msg["event_type"],
                    "domainName": msg["domain_name"],
                    "domainId": msg["domain_id"],
                    "payload": payload_data,
                    "dependentServiceName": msg["dependent_service_name"],
                    "isProcessed": msg["is_processed"]
                }

                rows.append(json_obj)

            logger.info(f"Retrieved {len(rows)} unprocessed outbox records from {schema} service")
            return rows
        except Exception as e:
            logger.error(f"Error reading from outbox: {e}")
            return []
        finally:
            # conn.close()
            cursor.close()
            db_conn_pool.putconn(conn)
            logger.info("Closed PostgreSQL connection")

    @task
    def publish_messages(msgs):
        if not msgs:
            logger.info("No messages to publish")
            return

        logger.info("Starting to connect to RabbitMQ")
        credentials = pika.PlainCredentials(
            Variable.get('ec_rabbitmq_username'),
            Variable.get('ec_rabbitmq_password')
        )
        
        try:
            rabbit_conn = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=Variable.get('ec_rabbitmq_host'),
                    port=int(Variable.get('ec_rabbitmq_port')),
                    credentials=credentials
                )
            )
            channel = rabbit_conn.channel()
            channel.exchange_declare(
                exchange=Variable.get('ec_rabbitmq_topic_exchange'),
                exchange_type='topic',
                durable=True
            )
            logger.info("Successfully connected to RabbitMQ and declared exchange")

            logger.info(f"Total number of messages to publish: {len(msgs)}")
            for msg in msgs:
                channel.basic_publish(
                    exchange=Variable.get('ec_rabbitmq_topic_exchange'),
                    routing_key=Variable.get('ec_rabbitmq_routing_key'),
                    body=json.dumps(msg),
                    properties=pika.BasicProperties(delivery_mode=2)  # Makes the message persistent
                )
            logger.info("Successfully published all outbox tasks to the queue")
        except Exception as e:
            logger.error(f"Error while publishing messages to RabbitMQ: {e}")
        finally:
            if 'rabbit_conn' in locals() and rabbit_conn.is_open:
                rabbit_conn.close()
                logger.info("Closed RabbitMQ connection")

    # rabbitmq_config = print_config()
    core_outbox_rows = read_outbox('core')
    master_data_outbox_rows = read_outbox('master_data')
    core_outbox_rows.concat(master_data_outbox_rows)
    publish_messages(core_outbox_rows)

# Instantiate the DAG
publish_outbox_task_instance = publish_outbox_task()