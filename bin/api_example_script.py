import pprint

import cct_service_alerts

with cct_service_alerts.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = cct_service_alerts.V13Api(api_client)
    unplanned_alerts = api_instance.v13_service_alerts_time_frame_planned_get("current", "unplanned")

pprint.pprint(unplanned_alerts[:2])
