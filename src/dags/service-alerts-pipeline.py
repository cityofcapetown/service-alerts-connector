from pipeline_utils import kubernetes_dag

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2024, 3, 5)
DAG_IMAGE = "cityofcapetown/datascience:python@sha256:ee64d1427e1906876d4c1dc426bdf1223dce7caf8c426a707be9f9c9d34af8b1"
DAG_OWNER = kubernetes_dag.DagOwner('ginggs', 'gordon.inggs@capetown.gov.za')

with kubernetes_dag.airflowK8sDAG("service-alerts-pipeline",
                                  DAG_OWNER, secret_name='service-alerts-secret',
                                  dag_image=DAG_IMAGE,
                                  start_date=DAG_STARTDATE, schedule_interval=timedelta(minutes=10),
                                  code_location='https://lake.capetown.gov.za/service-alerts.deploy/service-alerts.zip',
                                  concurrency=2) as dag:
    # Operators
    fetch_data_operator = dag.get_dag_operator("fetch-service-alerts",
                                               "python3 cct_connector/ServiceAlertConnector.py",
                                               resources=kubernetes_dag.LIGHT_RESOURCES)
    fix_data_operator = dag.get_dag_operator("fix-service-alerts",
                                             "python3 cct_connector/ServiceAlertFixer.py",
                                             resources=kubernetes_dag.LIGHT_RESOURCES)
    augment_data_operator = dag.get_dag_operator("augment-service-alerts",
                                                 "python3 cct_connector/ServiceAlertAugmenter.py",
                                                 resources=kubernetes_dag.LIGHT_RESOURCES)
    broadcast_data_operator = dag.get_dag_operator("broadcast-service-alerts",
                                                   "python3 cct_connector/ServiceAlertBroadcaster.py",
                                                   resources=kubernetes_dag.LIGHT_RESOURCES)
    email_data_operator = dag.get_dag_operator("email-service-alerts",
                                               "python3 cct_connector/ServiceAlertEmailer.py",
                                               resources=kubernetes_dag.LIGHT_RESOURCES)

    # Dependencies
    fetch_data_operator >> fix_data_operator >> augment_data_operator >> broadcast_data_operator >> email_data_operator
