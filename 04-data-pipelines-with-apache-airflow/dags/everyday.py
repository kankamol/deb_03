from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

def _world():
    print("world")

with DAG(
    dag_id="everyday",
    schedule="0 18 3 * *",
    start_date=timezone.datetime(2023, 5, 1),
    tags=["DEB", "Skooldio"],
    catchup=False,
):

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'hello '",
    )

    world = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    naja = BashOperator(
        task_id="naja",
        bash_command="echo 'naja '",
    )

    dot = BashOperator(
        task_id="dot",
        bash_command="echo '.'",
    )

    dotdot = BashOperator(
        task_id="dotdot",
        bash_command="echo '..'",
    )
    
    hello >> world >> naja >> dot >> dotdot