import pprint

import cct_service_alerts

with cct_service_alerts.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = cct_service_alerts.V1Api(api_client)
    unplanned_alerts = api_instance.v1_coct_service_alerts_current_unplanned_json_get()

pprint.pprint(unplanned_alerts)
