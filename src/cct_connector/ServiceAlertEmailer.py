import dataclasses
import functools
import hashlib
import logging
import pathlib
import tempfile
import typing
import uuid

from db_utils import exchange_utils, minio_utils, proxy_utils, secrets_utils
from exchangelib import HTMLBody, FileAttachment, Message
import jinja2
import pandas
import requests

from cct_connector import (
    TWEET_COL, IMAGE_COL,
    SA_EMAIL_NAME
)
from cct_connector.ServiceAlertBroadcaster import ServiceAlertOutputFileConfig, ServiceAlertBroadcaster, V0_COLS, ID_COL

DS_REPLY_TO = (
    "gordon.inggs@capetown.gov.za",
    "delyno.dutoit@capetown.gov.za",
    "henri.knoesen@capetown.gov.za",
    "kathryn.mcdermott@capetown.gov.za",
    "muhammed.ockards@capetown.gov.za",
)
RESOURCES_PATH = pathlib.Path(__file__).parent / ".." / "resources"
ALERT_EMAIL_SUBJECT_PREFIX = "Service Alert"
ALERT_EMAIL_TEMPLATE = "service_alert_tweet_emailer_template.html.jinja2"
CITY_LOGO_FILENAME = "rect_city_logo.png"
LINK_TEMPLATE = "https://ctapps.capetown.gov.za/sites/crhub/SitePages/ViewServiceAlert.aspx#?ID={alert_id}"
AREA_IMAGE_FILENAME = "area_image_filename.png"
IMAGE_LINK_TEMPLATE = "https://lake.capetown.gov.za/service-alerts.maps/{image_filename}"
EMAIL_LINK_TEMPLATE = "https://lake.capetown.gov.za/service-alerts.service-alerts-emails/{email_filename}"


@dataclasses.dataclass
class ServiceAlertEmailConfig(ServiceAlertOutputFileConfig):
    receivers: typing.Tuple[typing.Tuple[str or None, str], ...]
    email_focus: str
    additional_filter: str or typing.Callable or None

    def apply_additional_filter(self, data_df: pandas.DataFrame) -> pandas.DataFrame:
        logging.debug(f"( pre-filter) {data_df.shape=}")
        filtered_df = data_df.copy()

        if isinstance(self.additional_filter, str):
            logging.debug("Applying query")
            filtered_df = data_df.query(self.additional_filter).copy()
        if isinstance(self.additional_filter, typing.Callable):
            filtered_df = data_df.loc[
                data_df.apply(self.additional_filter, axis=1)
            ].copy()

        logging.debug(f"(post filter) {filtered_df.shape=}")

        return filtered_df


EMAIL_COLS = [ID_COL, "service_area", "title", "description",
              "area_type", "area", "location",
              "inferred_wards", "inferred_suburbs", IMAGE_COL,
              "start_timestamp", "forecast_end_timestamp",
              "planned", "request_number", TWEET_COL]


def _ward_curry_pot(ward_number: str) -> typing.Callable[[pandas.Series], bool]:
    # creating curried filter function
    def _ward_filter(row: pandas.Series) -> bool:
        return (row["inferred_wards"] is not None and
                ward_number in row["inferred_wards"] and
                # citywide alerts not to be trusted, yet
                row["area_type"] != "Citywide")

    return _ward_filter


def _service_area_curry_pot(service_area: str) -> typing.Callable[[pandas.Series], bool]:

    # creating curried filter function
    def _service_area_filter(row: pandas.Series) -> bool:
        return (row["service_area"] is not None and
                service_area == row["service_area"])

    return _service_area_filter


SA_EMAIL_CONFIGS = [
    # All Alerts
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Social Media Team", "social.media@capetown.gov.za"),),
                            "all unplanned alerts",
                            None),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Social Media Team", "social.media@capetown.gov.za"),),
                            "all planned alerts",
                            None),
    # Electricity-specific
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Mary-Ann", "MaryAnn.FransmanJohannes@capetown.gov.za"),
                             ("Electricity Maintenance Team", "ElectricityMaintenance.Outages@capetown.gov.za"),),
                            "all planned electricity work",
                            _service_area_curry_pot("Electricity")),
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Liza", "Elizabeth.Laubscher@capetown.gov.za"),
                             ("Jean-Marie", "JeanMarie.deWaal@capetown.gov.za"),
                             ("Michelle", "MichelleMargaret.Jones@capetown.gov.za"),
                             ("Aidan", "AidanKarl.vandenHeever@capetown.gov.za"),),
                            "all unplanned electricity alerts",
                            _service_area_curry_pot("Electricity")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Liza", "Elizabeth.Laubscher@capetown.gov.za"),
                             ("Jean-Marie", "JeanMarie.deWaal@capetown.gov.za"),
                             ("Michelle", "MichelleMargaret.Jones@capetown.gov.za"),
                             ("Aidan", "AidanKarl.vandenHeever@capetown.gov.za"),),
                            "all planned electricity alerts",
                            _service_area_curry_pot("Electricity")),
    # Electricity-specific
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Melissa", "Melissa.DeSousaAlves@capetown.gov.za"),),
                            "all planned water and sanitation work",
                            _service_area_curry_pot("Water & Sanitation")),
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Melissa", "Melissa.DeSousaAlves@capetown.gov.za"),),
                            "all unplanned water and sanitation alerts",
                            _service_area_curry_pot("Water & Sanitation")),
    # Ward 115
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr McMahon", "Ian.McMahon@capetown.gov.za"),
                             ("Girshwin", "girshwin.fouldien@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 115",
                            _ward_curry_pot("115")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr McMahon", "Ian.McMahon@capetown.gov.za"),
                             ("Girshwin", "girshwin.fouldien@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all planned works that might affect Ward 115",
                            _ward_curry_pot("115")),
    # Ward 16
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Barends", "ursula.barends@capetown.gov.za"),
                             ("Lorraine", "Lorraine.Frost@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 16",
                            _ward_curry_pot("16")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Barends", "ursula.barends@capetown.gov.za"),
                             ("Lorraine", "Lorraine.Frost@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all planned works that might affect Ward 16",
                            _ward_curry_pot("16")),
    # Ward 77
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Higham", "Francine.Higham@capetown.gov.za"),
                             ("Girshwin", "girshwin.fouldien@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 77",
                            _ward_curry_pot("77")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Higham", "Francine.Higham@capetown.gov.za"),
                             ("Girshwin", "girshwin.fouldien@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all planned works that might affect Ward 77",
                            _ward_curry_pot("77")),
    # Ward 21
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Terblanche", "hendri.terblanche@capetown.gov.za"),
                             ("Carin", "Carin.Viljoen@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 21",
                            _ward_curry_pot("21")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Terblanche", "hendri.terblanche@capetown.gov.za"),
                             ("Carin", "Carin.Viljoen@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Delyno", "delyno.dutoit@capetown.gov.za"),
                             ("Henri", "henri.knoesen@capetown.gov.za"),
                             ("Kathryn", "kathryn.mcdermott@capetown.gov.za"),
                             ("Muhammed", "muhammed.ockards@capetown.gov.za"),),
                            "all planned works that might affect Ward 21",
                            _ward_curry_pot("21")),
    # Somerset West
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Delyno", "delyno.dutoit@capetown.gov.za"),),
                            "all unplanned alerts that affect Somerset West",
                            "(inferred_suburbs.astype('str').str.lower().str.contains('somerset\Wwest') or "
                            " area.astype('str').str.lower().str.contains('somerset\Wwest')) and "
                            "area_type != 'Citywide'"),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Delyno", "delyno.dutoit@capetown.gov.za"),),
                            "all planned works that affect Somerset West",
                            "(inferred_suburbs.astype('str').str.lower().str.contains('somerset\Wwest') or "
                            " area.astype('str').str.lower().str.contains('somerset\Wwest')) and "
                            "area_type != 'Citywide'"),
    # Citywide
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Shereef", "Mohammed.Maroof@capetown.gov.za"),),
                            "all unplanned alerts affecting the whole City",
                            "area_type == 'Citywide'"),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Gordon", "gordon.inggs@capetown.gov.za"),
                             ("Shereef", "Mohammed.Maroof@capetown.gov.za"),),
                            "all planned works affecting the whole City",
                            "area_type == 'Citywide'"),
]


@functools.lru_cache()
def _get_image_attachment(image_path: pathlib.Path, attachment_name: str) -> FileAttachment:
    with open(image_path, "rb") as image_file:
        logo_attachment = FileAttachment(name=attachment_name, content=image_file.read(), is_inline=True)

    return logo_attachment


def _form_and_send_alerts_email(alert_dict: typing.Dict[str, typing.Any],
                                email_focus: str,
                                email_filename: str,
                                recipients: typing.Tuple[typing.Tuple[str, str]],
                                http_session: requests.Session) -> str:
    secrets = secrets_utils.get_secrets()

    with proxy_utils.set_env_http_proxy():
        account = exchange_utils.setup_exchange_account(secrets["proxy"]["username"],
                                                        secrets["proxy"]["password"], )

        # Forming email message
        email_subject = f"{ALERT_EMAIL_SUBJECT_PREFIX} - {alert_dict['title']} in {alert_dict['area']}"
        email_request_id = str(uuid.uuid4())
        email_date = pandas.Timestamp.now().isoformat()
        suggested_post = alert_dict[TWEET_COL]
        link_str = LINK_TEMPLATE.format(alert_id=alert_dict[ID_COL])
        image_link_str = (
            IMAGE_LINK_TEMPLATE.format(image_filename=alert_dict[IMAGE_COL])
            if alert_dict[IMAGE_COL] is not None
            else None
        )

        # removing null fields and tweet col for email generation
        fields_to_delete = [TWEET_COL, IMAGE_COL]
        for k, v in alert_dict.items():
            if not isinstance(v, typing.Collection) and pandas.isna(v):
                fields_to_delete += [k]
            elif isinstance(v, typing.Collection) and all(map(pandas.isna, v)):
                fields_to_delete += [k]

        if alert_dict["area_type"] == "Official Planning Suburb":
            fields_to_delete += ["inferred_suburbs"]
        elif alert_dict["area_type"] == "Citywide":
            fields_to_delete += ["inferred_suburbs", "inferred_wards"]

        for k in fields_to_delete:
            if k in alert_dict:
                del alert_dict[k]

        # formatting array fields
        for k, v in alert_dict.items():
            if isinstance(v, typing.Collection) and not isinstance(v, str):
                alert_dict[k] = ", ".join(v)

        logging.debug(f"{email_subject=}, {email_request_id=}, {email_date=}")

        logging.debug("Forming email body")
        with open(RESOURCES_PATH / ALERT_EMAIL_TEMPLATE, 'r') as template_file:
            message_body = jinja2.Template(template_file.read()).render(
                email_subject=email_subject,
                recipients=[name for name, _ in recipients if name],
                alert_dict=alert_dict,
                post_text=suggested_post,
                email_focus=email_focus,
                request_id=email_request_id,
                iso8601_timestamp=email_date,
                bok_link=link_str,
                image_path=image_link_str,
                email_link=EMAIL_LINK_TEMPLATE.format(email_filename=email_filename)
            )

        logging.debug("Creating email")
        message = Message(account=account,
                          body=HTMLBody(message_body),
                          subject=email_subject,
                          to_recipients=[email for _, email in recipients],
                          reply_to=DS_REPLY_TO)

        # Attaching logo
        logo_path = RESOURCES_PATH / CITY_LOGO_FILENAME
        message.attach(_get_image_attachment(logo_path, CITY_LOGO_FILENAME))

        # Attaching area image
        if image_link_str:
            with tempfile.NamedTemporaryFile("wb") as image_temp_file:
                image_temp_file.write(http_session.get(image_link_str).content)
                image_temp_file.flush()

                message.attach(_get_image_attachment(pathlib.Path(image_temp_file.name).absolute(),
                                                     AREA_IMAGE_FILENAME))

        logging.debug("Sending email")
        message.send()

        return message_body


class ServiceAlertEmailer(ServiceAlertBroadcaster):
    def __init__(self, minio_write_name=SA_EMAIL_NAME):
        super().__init__(minio_write_name=minio_write_name)

    def send_alert_emails(self):
        with proxy_utils.setup_http_session() as http:
            for config, (_, alert_df) in zip(SA_EMAIL_CONFIGS,
                                             self._service_alerts_generator(SA_EMAIL_CONFIGS)):
                config_hash = hashlib.sha256(str.encode(str(config.receivers) +
                                                        str(config.email_focus))).hexdigest()
                if alert_df.empty:
                    logging.warning(f"Nothing more to do for {config=}, skipping!")
                    continue

                alert_df = config.apply_additional_filter(alert_df)

                for alert_dict in alert_df.to_dict(orient="records"):
                    email_filename = f"{config_hash}_{alert_dict[ID_COL]}.html"

                    if alert_dict[TWEET_COL] is None:
                        logging.warning(f"Skipping empty post - {alert_dict[ID_COL]}")
                        continue

                    logging.debug("Checking if email has already been sent...")
                    skip_flag = False
                    for _ in minio_utils.list_objects_in_bucket(self.minio_write_name,
                                                                minio_prefix_override=email_filename):
                        logging.warning(f"Skipping {alert_dict[ID_COL]} for this config - already sent!")
                        skip_flag = True
                        break

                    if skip_flag:
                        continue

                    email_message = _form_and_send_alerts_email(alert_dict, config.email_focus, email_filename,
                                                                config.receivers,
                                                                http)

                    logging.debug("Backing up email")
                    with tempfile.TemporaryDirectory() as tempdir:
                        local_path = pathlib.Path(tempdir) / email_filename
                        with open(local_path, "w") as local_file:
                            local_file.write(email_message)

                        minio_utils.file_to_minio(local_path, self.minio_write_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    logging.info("G[etting] data from Minio...")
    sa_emailer = ServiceAlertEmailer()
    logging.info("...G[ot] data from Minio")

    logging.info("Sen[ding] emails...")
    sa_emailer.send_alert_emails()
    logging.info("...Sen[t] emails")
