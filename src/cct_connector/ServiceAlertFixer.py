from datetime import datetime
import logging
import re

from db_utils import minio_utils
import pandas as pd
import pyarrow.dataset as ds
import pytz

from cct_connector import ServiceAlertBase
from cct_connector import (
    RAW_SA_NAME, FIXED_SA_NAME,
    ID_COL,
    FIXED_SN_MINIO_NAME
)

SN_REGEX_PATTERN = r"^\d{10}$"
SN_RE = re.compile(SN_REGEX_PATTERN)
HM_RE = re.compile(r'^\d{2}:\d{2}$')
SAST_TZ = pytz.timezone('Africa/Johannesburg')


def _clean_sa_df(data_df: pd.DataFrame) -> pd.DataFrame:
    logging.debug("Cleaning DF...")
    cleaned_df = data_df.assign(**{
        # cleaning up notification number reference
        "notification_number": lambda df: df["Reference_x0020_No"].apply(
            lambda val: f"{int(val):012d}" if val and SN_RE.match(val) else None
        ),
        # turning planned vs unplanned into boolean
        "planned": lambda df: df["Planned_x0020_Unplanned"].map({"Planned": True, "Unplanned": False}),
        # converting various dates into SAST
        "publish_date": lambda df: pd.to_datetime(df["Publish_x0020_Date"]).dt.tz_convert("+02:00"),
        "effective_date": lambda df: pd.to_datetime(df["Effective_x0020_Date"]).dt.tz_convert("+02:00"),
        # increment expiry date by a single day to indicate,
        # as this is the date **after** which the notification expires
        "expiry_date": lambda df: (
                pd.to_datetime(df["Alert_x0020_Expiry_x0020_Date"]).dt.tz_convert("+02:00") + pd.Timedelta(days=1)
        ),
        # computing duration
        "duration": lambda df: df["expiry_date"] - df["publish_date"],
        # Munging times
        "start_time": lambda df: df["Start_x0020_Time"].apply(
            lambda val: datetime.strptime(val.replace("60", "59").replace("Select...","00") + "+02:00", "%H:%M%z")
        ).dt.time,
        "forecast_end_time": lambda df: df["Forecast_x0020_End_x0020_Time"].apply(
            lambda val: (
                datetime.strptime(
                    val.replace("60", "59").replace("Select...","00") + "+02:00", "%H:%M%z"
                ) if (pd.notna(val) and HM_RE.match(val)) else None
            )
        ),
        # Creating timestamps
        "start_timestamp": lambda df: df.apply(
            lambda row: datetime.combine(row["effective_date"], row["start_time"]),
            axis=1
        ).dt.tz_localize("+02:00"),
        "forecast_end_timestamp": lambda df: df.apply(
            lambda row: SAST_TZ.localize(datetime.combine(
                # Assuming that it ends on the day of expiry
                (row["expiry_date"] - pd.Timedelta(days=1)).date(),
                row["forecast_end_time"].time()
            )) if pd.notna(row['forecast_end_time']) else None,
            axis=1
        ),
        "location": lambda df: df.apply(
            lambda row: (
                # dropping location entry if it overlaps with the description entry
                row["Address_x0020_Location_x0020_2"].strip() if (
                        row["Address_x0020_Location_x0020_2"] and
                        row["Description12"] and
                        row["Address_x0020_Location_x0020_2"][:len(row["Description12"])] !=
                        row["Description12"][:len(row["Address_x0020_Location_x0020_2"])]
                ) else
                # Using controlled address field
                row["All_x0020_Location_x0020_Selected"].strip() if (
                    row["All_x0020_Location_x0020_Selected"]
                ) else None
            ), axis=1
        )
    }).assign(**{
        # fixing cases where the start and end timestamps roll over the day
        "forecast_end_timestamp": lambda df: df.apply(
            lambda row: (row["forecast_end_timestamp"] + pd.Timedelta(days=(
                1 if row["forecast_end_timestamp"] <= row["start_timestamp"] else 0
            ))) if pd.notna(row["forecast_end_timestamp"]) else None,
            axis=1
        ),
    }).rename(columns={
        "Service_x0020_Area12": "service_area",
        "Title1": "title",
        "Description12": "description",
        "Status12": "status",
        "Area": "area",
        "Areatype": "area_type",
        "Subtitle": "subtitle"
    })[[
        ID_COL,
        "service_area", "title", "subtitle", "description",
        "area_type", "area", "location",
        "publish_date", "effective_date", "expiry_date", "start_timestamp", "forecast_end_timestamp",
        "planned", "status", "notification_number",
    ]]
    logging.debug("...Cleaned DF")
    logging.debug(f"cleaned_df.sample(5)=\n{cleaned_df.sample(5)}")

    return cleaned_df


def _lookup_request_number(data_df: pd.DataFrame) -> pd.DataFrame:
    logging.debug("Fetching lookup DF")
    lookup_df = minio_utils.minio_to_dataframe(
        FIXED_SN_MINIO_NAME,
        columns=["ReferenceNumber"],
        filters=ds.field("ReferenceNumber").is_valid(),
    ).dropna().rename(
        columns={"ReferenceNumber": "request_number"}
    )
    logging.debug("Fetched lookup DF")
    logging.debug(f"lookup_df.sample(5)=\n{lookup_df.sample(5)}")

    logging.debug("Merging data...")
    merged_df = data_df.merge(
        lookup_df,
        left_on="notification_number", right_index=True,
        how="left", validate="many_to_one"
    )
    logging.debug("...Merged data")

    logging.debug(f"merged_df.sample(5)=\n{merged_df.sample(5)}")
    return merged_df


class ServiceAlertFixer(ServiceAlertBase.ServiceAlertsBase):
    def __init__(self, minio_read_name=RAW_SA_NAME, minio_write_name=FIXED_SA_NAME):
        super().__init__(None, None, minio_utils.DataClassification.LAKE,
                         minio_read_name=minio_read_name, minio_write_name=minio_write_name,
                         use_cached_values=False)

        self.data = self.get_data_from_minio()

    def clean_df(self):
        self.data = _clean_sa_df(self.data)

    def lookup_request_number(self):
        self.data = _lookup_request_number(self.data)

    def dedup_old_data(self):
        old_data = self.get_data_from_minio(self.minio_write_name)
        logging.debug(f"{old_data.shape=}")

        logging.debug(f"prior to dedup - {self.data.shape=}")
        self.data = pd.concat((
            old_data, self.data
        )).drop_duplicates(
            subset=[ID_COL,],
            keep='last'
        )
        logging.debug(f"    post dedup - {self.data.shape=}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    logging.info("G[etting] data from Minio...")
    bok_fixer = ServiceAlertFixer()
    logging.info("...G[ot] data from Minio")

    logging.info("Fix[ing] data...")
    bok_fixer.clean_df()
    bok_fixer.lookup_request_number()
    logging.info("...Fix[ed] data")

    logging.info("Back[ing] up old data...")
    bok_fixer.dedup_old_data()
    logging.info("...Back[ed] up old data")

    logging.info("Wr[iting] to Minio...")
    bok_fixer.write_data_to_minio(bok_fixer.data)
    logging.info("...Wr[ote] to Minio")
