import logging
import typing

from db_utils import minio_utils, sharepoint_utils, proxy_utils
import pandas as pd
import requests_ntlm

from cct_connector import ServiceAlertBase
from cct_connector import (
    RAW_SA_NAME,
)


SP_CRM_ALERTS_URL = "http://ctapps.capetown.gov.za/sites/crhub//_api/web/lists/getbytitle('Service Alerts')/items"
SP_CRM_COLS = (
    "id", "Title1", 'Service_x0020_Area12', 'Description12', 'Planned_x0020_Unplanned',
    'Area', 'Address_x0020_Location_x0020_2',
    'Publish_x0020_Date', 'Effective_x0020_Date', 'Start_x0020_Time', 'Forecast_x0020_End_x0020_Time',
    'Alert_x0020_Expiry_x0020_Date',
    'Reference_x0020_No', 'Status12',
)


def _get_creds() -> requests_ntlm.HttpNtlmAuth:
    sharepoint_username, sharepoint_password = sharepoint_utils._get_sharepoint_secrets_from_secrets(
        sharepoint_domain=sharepoint_utils.CITY_SHAREPOINT_AUTH_DOMAIN
    )
    return requests_ntlm.HttpNtlmAuth(sharepoint_username, sharepoint_password)


def _data_dict_cleaner(data_dict: typing.Dict) -> typing.Dict:
    for d in data_dict:
        del d['__metadata']
        cleaned_df = {
            k: v for k, v in d.items()
            # all columns that aren't empty
            if v is not None and
               # only take flat values
               not isinstance(v, dict)
        }
        yield cleaned_df

def _data_generator(creds: requests_ntlm.HttpNtlmAuth):
    with proxy_utils.setup_http_session() as http:
        initial_resp = http.get(
            SP_CRM_ALERTS_URL,
            headers={"accept": "application/json;odata=verbose"}, auth=creds
        )

        json_resp = initial_resp.json()

        data = json_resp['d']['results']
        for d in _data_dict_cleaner(data):
            yield d

        continuation_url = json_resp['d']['__next'] if '__next' in json_resp['d'] else None
        while continuation_url:
            next_resp = http.get(continuation_url, headers={"accept": "application/json;odata=verbose"}, auth=creds)
            json_resp = next_resp.json()
            data = json_resp['d']['results']
            for d in _data_dict_cleaner(data):
                yield d

            continuation_url = json_resp['d']['__next'] if '__next' in json_resp['d'] else None


class ServiceAlertConnector(ServiceAlertBase.ServiceAlertsBase):
    def __init__(self, minio_write_name=RAW_SA_NAME):
        self.data = None

        super().__init__(None, None,
                         minio_utils.DataClassification.LAKE, minio_write_name,
                         use_cached_values=False)

    def get_data_from_sp(self):
        creds = _get_creds()

        self.data = pd.DataFrame(_data_generator(creds)).dropna(subset=['Publish_x0020_Date'])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    sa_connector = ServiceAlertConnector()

    logging.info("Fetch[ing] data from sharepoint...")
    sa_connector.get_data_from_sp()
    logging.info("...Fetch[ed] data from sharepoint")

    logging.info("Wr[iting] data to Minio...")
    sa_connector.write_data_to_minio(sa_connector.data)
    logging.info("...Wr[ote] data to Minio...")
