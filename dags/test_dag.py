from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
import pendulum
from datetime import timedelta

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2023, 1, 1),
    'start_date': datetime(2024, 10, 27, 4, 50),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5)
}

# Define the DAG using the @dag decorator
@dag(
    dag_id='print_current_time_dag',
    default_args=default_args,
    # schedule_interval='@daily',
    catchup=False,
    tags=['example'],
    schedule_interval = timedelta(minutes = 5)
)
def print_time_dag():
    @task
    def print_current_time():
        now = datetime.now()
        print(f"The current time is: {now}")

    @task
    def add_two_numbers():
        print(f"Added result: 5 + 2 = 7")
    
    print_current_time()  # Call the task function within the DAG
    add_two_numbers()

# Instantiate the DAG
print_time_dag_instance = print_time_dag()
