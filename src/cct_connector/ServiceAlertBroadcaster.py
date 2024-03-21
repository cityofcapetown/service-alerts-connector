import dataclasses
import logging
import typing

import boto3.session
from db_utils import minio_utils, proxy_utils, secrets_utils
import pandas as pd

from cct_connector import ServiceAlertBase
from cct_connector import (
    AUGMENTED_SA_NAME, SERVICE_ALERTS_S3_BUCKET,
    ID_COL, TWEET_COL, TOOT_COL,
)


@dataclasses.dataclass
class ServiceAlertOutputFileConfig:
    time_window_size: typing.Union[int, str, None]
    planned: bool
    version: str
    columns: typing.List[str]

    def generate_filename(self):
        time_str = f"{self.time_window_size}days" if isinstance(self.time_window_size, int) else "all"
        time_str = self.time_window_size if isinstance(self.time_window_size, str) else time_str

        planned_str = "planned" if self.planned else "unplanned"

        filename = f"coct-service_alerts-{time_str}-{planned_str}.json"
        filepath = ((self.version + "/") if self.version != "v0" else "") + filename

        return filepath


V0_COLS = [ID_COL, "service_area", "title", "description",
           "area", "location",
           "publish_date", "effective_date", "expiry_date", "start_timestamp", "forecast_end_timestamp",
           "planned", "request_number", ]
V1_COLS = V0_COLS + [TWEET_COL, TOOT_COL]

BOK_CONFIGS = [
    ServiceAlertOutputFileConfig(time_window, planned, version, version_cols)
    for time_window in [None, 7, "current"]
    for planned in [True, False]
    for version, version_cols in (('v0', V0_COLS),
                                  ('v1', V1_COLS))
]

EXPIRY_COL = 'expiry_date'
PLANNED_COL = 'planned'


class ServiceAlertBroadcaster(ServiceAlertBase.ServiceAlertsBase):
    def __init__(self, minio_read_name=AUGMENTED_SA_NAME, minio_write_name=None):
        super().__init__(None, None, minio_utils.DataClassification.LAKE,
                         minio_read_name=minio_read_name, minio_write_name=minio_write_name,
                         use_cached_values=False)

        self.data = self.get_data_from_minio().reset_index()

    def _service_alerts_generator(self, configs=BOK_CONFIGS):
        now = pd.Timestamp.now(tz="Africa/Johannesburg")

        for bok_config in configs:
            config_ts = now
            if bok_config.time_window_size is None:
                config_ts = self.data[EXPIRY_COL].min() - pd.Timedelta(days=1)
            elif isinstance(bok_config.time_window_size, int):
                config_ts -= pd.Timedelta(days=bok_config.time_window_size)
            logging.debug(f"{config_ts=}")

            # Time filtering
            filtered_df = self.data.query(f"{EXPIRY_COL} > @config_ts")

            # Planned vs Unplanned
            filtered_df = filtered_df.query(f"{PLANNED_COL} == {bok_config.planned}")

            output_df = filtered_df[bok_config.columns]
            logging.debug(f"{output_df.shape=}")

            yield bok_config.generate_filename(), output_df

    def write_to_s3(self):
        secrets = secrets_utils.get_secrets()
        s3_secrets = secrets["aws"]["s3"]

        with proxy_utils.set_env_http_proxy():
            boto_session = boto3.session.Session(aws_access_key_id=s3_secrets["access"],
                                                 aws_secret_access_key=s3_secrets["secret"],
                                                 region_name="af-south-1")
            s3 = boto_session.resource('s3')

            for obj_name, output_df in self._service_alerts_generator():
                logging.debug(f"Writing {obj_name} to S3")
                output_json = output_df.to_json(orient='records', date_format='iso')
                s3.Bucket(SERVICE_ALERTS_S3_BUCKET).put_object(Body=output_json, Key=obj_name,
                                                               ContentType="application/json")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    logging.info("G[etting] data from Minio...")
    sa_broadcaster = ServiceAlertBroadcaster()
    logging.info("...G[ot] data from Minio")

    logging.info("Wr[iting] to S3")
    sa_broadcaster.write_to_s3()
    logging.info("Wr[ote] to S3")
