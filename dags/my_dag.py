from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import random
import string


default_args = {
    'owner': 'pinto',
    'retries': 4,
    'retry_delay': timedelta(seconds=5),
}


def print_message(ti):
    age = ti.xcom_pull(task_ids="first_task", key="age")
    mess = "Hello my name is " + age + ", and please dont forget i love you all"
    print(mess)
    

def return_name(ti):
    ti.xcom_push(key="age", value=34)
    ti.xcom_push(key="name", value="Aaron")
    print("DELIVERED")    
    

def generate_password(length,ti):
    characters = string.ascii_letters + string.digits
    password = ''.join(random.choice(characters) for number in range(length))
    ti.xcom_push(key="password", value=password)
    print(f"MY GENERATED PASSWORD IS {password}")

def print_story(ti):
    name = ti.xcom_pull(task_ids="first_task", key="name")
    password = ti.xcom_pull(task_ids="second_task", key="password")
    print(f"This is {name} and i have been compromised with {password}")


with DAG(
    dag_id="aaron_dag_v3",
    start_date=datetime(2023, 11, 10),
    schedule_interval="@daily",
    catchup=False
) as dag:
    first_task = PythonOperator(
        task_id="first_task",
        python_callable=return_name,
    )

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=generate_password,
        op_kwargs={"length": 18}
    )

    third_task = PythonOperator(
        task_id="third_task",
        python_callable=print_message,
    )

    fourth_task = PythonOperator(
        task_id="fourth_task",
        python_callable=print_story,
    )

    first_task.set_downstream(second_task)
    first_task.set_downstream(third_task)
    second_task.set_downstream(fourth_task)
