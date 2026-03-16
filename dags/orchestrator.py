from airflow.sdk import dag
from extract import arxiv_paper_extractor
from transform import arxiv_paper_transformer
from load import arxiv_paper_loader
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    schedule="@daily",
    tags=['arxiv', 'orchestrator']
)
def arxiv_etl_orchestrator():

    extract_dag = TriggerDagRunOperator(
        task_id='trigger_extract',
        trigger_dag_id='arxiv_paper_extractor',
        wait_for_completion=True
    )

    transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='arxiv_paper_transformer',
        wait_for_completion=True
    )

    load_dag = TriggerDagRunOperator(
        task_id='trigger_load',
        trigger_dag_id='arxiv_paper_loader',
        wait_for_completion=True
    )

    extract_dag >> transform_dag >> load_dag

arxiv_etl_orchestrator()
