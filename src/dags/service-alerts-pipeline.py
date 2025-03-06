from pipeline_utils import kubernetes_dag

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2024, 3, 5)
DAG_IMAGE = "cityofcapetown/datascience:python@sha256:269abfbfd016f8b082c39bbe4e7d6a0850172ef88e7ddadb84ce4239329001e6"
DAG_OWNER = kubernetes_dag.DagOwner('ginggs', 'gordon.inggs@capetown.gov.za')

with kubernetes_dag.airflowK8sDAG("service-alerts-pipeline",
                                  DAG_OWNER, secret_name='service-alerts-connector-secret',
                                  dag_image=DAG_IMAGE,
                                  start_date=DAG_STARTDATE, schedule_interval=timedelta(minutes=10),
                                  code_location='https://lake.capetown.gov.za/service-alerts-connector.deploy/service-alerts-connector.zip',
                                  install_gs_utils=True,
                                  concurrency=2,
                                  max_active_runs=2) as dag:
    # Operators
    fetch_data_operator = dag.get_dag_operator("fetch-service-alerts",
                                               "python3 cct_connector/ServiceAlertConnector.py",
                                               resources=kubernetes_dag.LIGHT_RESOURCES,
                                               startup_timeout_seconds=600,
                                               task_concurrency=1, )
    fix_data_operator = dag.get_dag_operator("fix-service-alerts",
                                             "python3 cct_connector/ServiceAlertFixer.py",
                                             resources=kubernetes_dag.LIGHT_RESOURCES,
                                             startup_timeout_seconds=600,
                                             task_concurrency=1, )
    augment_data_operator = dag.get_dag_operator("augment-service-alerts",
                                                 "python3 cct_connector/ServiceAlertAugmenter.py",
                                                 resources=kubernetes_dag.LIGHT_RESOURCES,
                                                 startup_timeout_seconds=600,
                                                 task_concurrency=1, )
    broadcast_data_operator = dag.get_dag_operator("broadcast-service-alerts",
                                                   "python3 cct_connector/ServiceAlertBroadcaster.py",
                                                   resources=kubernetes_dag.LIGHT_RESOURCES,
                                                   startup_timeout_seconds=600,
                                                   task_concurrency=1, )
    email_data_operator = dag.get_dag_operator("email-service-alerts",
                                               "python3 cct_connector/ServiceAlertEmailer.py",
                                               resources=kubernetes_dag.LIGHT_RESOURCES,
                                               startup_timeout_seconds=600,
                                               task_concurrency=1, )

    # Dependencies
    fetch_data_operator >> fix_data_operator >> augment_data_operator >> (broadcast_data_operator, email_data_operator)
