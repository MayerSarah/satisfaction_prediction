from airflow import DAG
from datetime import datetime

with DAG(
    "unsatisfied_opportunities",
    description="Detection de dossiers insatisfaits dans les opportunités récentes ayant signé un mandat",
    start_date=datetime(2022, 6, 20, 3, 0),
    schedule_interval="@daily",  # every day
    catchup=False,
) as dag:

    from score_satisfaction_V2.predict import get_predict_probability_recent
    from score_satisfaction_V2.alert_satisfaction import check_satisfaction
    from airflow.operators.python import PythonOperator

    detect_unsatisfied = PythonOperator(
        task_id="compute_proba_unsatisfied_opportunity",
        python_callable=get_predict_probability_recent,
        dag=dag,
    )

    update_unsatisfied = PythonOperator(
        task_id="update_gsheet_unsatisfied_opportunity",
        python_callable=check_satisfaction,
        dag=dag,
    )

detect_unsatisfied >> update_unsatisfied
