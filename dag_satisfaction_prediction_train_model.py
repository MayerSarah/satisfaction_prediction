from airflow import DAG
from datetime import datetime

with DAG(
    "unsatisfied_model",
    description="Entrainement du modèle de satisfaction",
    start_date=datetime(2022, 6, 20, 3, 0),
    schedule_interval="0 3 1,15 * *",  # twice a month
    catchup=False,
) as dag:

    from score_satisfaction_V2.train import train_model
    from airflow.operators.python import PythonOperator

    train_model_again = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        dag=dag,
    )

    train_model_again
