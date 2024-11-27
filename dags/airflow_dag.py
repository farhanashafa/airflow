from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pika
import psycopg2
from datetime import datetime

# Function to read data from the database and publish to RabbitMQ
def read_and_publish():
    # Database connection
    # db = Database()
    print('running dag!!!!')
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Read from outbox table
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM outbox where is_published = false")  # Adjust the query as needed
    messages = cursor.fetchall()
    print('Fetched all messages', messages)
    
    # RabbitMQ connection
    rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(host = RABBITMQ_CONFIG['host']))  # Change host if necessary
    channel = rabbit_conn.channel()
    channel.queue_declare(queue = RABBITMQ_CONFIG['queue_name'])  # Ensure the queue exists

    for message in messages:
        # Publish each message to RabbitMQ
        channel.basic_publish(exchange = '', routing_key = RABBITMQ_CONFIG['queue_name'], body = str(message[1]))
        print(f"Published message: {message[0]}, {type(message[0])}")
        print(f"Published message: {message[1]}, {type(message[1])}")

    # Clean up
    cursor.close()
    rabbit_conn.close()


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 22, 1, 10), #'2024-10-22',  # Set the starting date of your DAG
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    'catchup':False
}

# Create the DAG
dag = DAG(
    'database_to_rabbitmq',
    default_args = default_args,
    description = 'A simple DAG to read from outbox table and publish to RabbitMQ every 5 minutes',
    schedule_interval = timedelta(minutes = 5),  # Schedule the DAG to run every 5 minutes
)

# Define the PythonOperator task
read_publish_task = PythonOperator(
    task_id = 'read_and_publish',
    python_callable = read_and_publish,
    dag = dag,
)



# Configuration for database and RabbitMQ

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'outbox',
    'user': 'root',
    'password': '123456',
}

RABBITMQ_CONFIG = {
    'host': 'localhost',
    'port': 5672,
    'queue_name': 'test_queue',
}
