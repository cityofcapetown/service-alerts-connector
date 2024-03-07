import functools
import dataclasses
import logging
import pathlib
import tempfile
import typing
import uuid

from db_utils import exchange_utils, minio_utils, proxy_utils, secrets_utils
from exchangelib import HTMLBody, FileAttachment, Message
import jinja2
import pandas

from cct_connector import (
    TWEET_COL,
    SA_EMAIL_NAME
)
from cct_connector.ServiceAlertBroadcaster import ServiceAlertOutputFileConfig, ServiceAlertBroadcaster, V0_COLS, ID_COL

DS_REPLY_TO = (
    "gordon.inggs@capetown.gov.za",
    "delyno.dutoit@capetown.gov.za"
)
RESOURCES_PATH = pathlib.Path(__file__).parent / ".." / "resources"
ALERT_EMAIL_SUBJECT_PREFIX = "Service Alert Emailer"
ALERT_EMAIL_TEMPLATE = "service_alert_tweet_emailer_template.html.jinja2"
CITY_LOGO_FILENAME = "rect_city_logo.png"
LINK_TEMPLATE = "https://ctapps.capetown.gov.za/sites/crhub/SitePages/ViewServiceAlert.aspx#?ID={alert_id}"


@dataclasses.dataclass
class ServiceAlertEmailConfig(ServiceAlertOutputFileConfig):
    receivers: typing.Tuple[typing.Tuple[str or None, str], ...]
    subject_suffix: str
    additional_filter: str or None


SA_EMAIL_CONFIGS = [
    # Planned Electricity Alerts
    ServiceAlertEmailConfig("current", True, "v1", V0_COLS + [TWEET_COL],
                            (("Mary-Ann", "MaryAnn.FransmanJohannes@capetown.gov.za"),
                             (None, "ElectricityMaintenance.Outages@capetown.gov.za"),
                             (None, "social.media@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),),
                            "planned electricity alerts",
                            "service_area == 'Electricity'"),
    # All other Planned Alerts
    ServiceAlertEmailConfig("current", True, "v1", V0_COLS + [TWEET_COL],
                            ((None, "social.media@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),),
                            "planned alerts",
                            "service_area != 'Electricity'"),
    # Unplanned Alerts
    ServiceAlertEmailConfig("current", False, "v1", V0_COLS + [TWEET_COL],
                            ((None, "social.media@capetown.gov.za"),
                             ("Gordon", "gordon.inggs@capetown.gov.za"),),
                            "unplanned alerts",
                            None),
]


@functools.lru_cache(1)
def _get_logo_attachment() -> FileAttachment:
    logo_path = RESOURCES_PATH / CITY_LOGO_FILENAME
    with open(logo_path, "rb") as logo_file:
        logo_attachment = FileAttachment(name=CITY_LOGO_FILENAME, content=logo_file.read(), is_inline=True)

    return logo_attachment


def _form_and_send_alerts_email(alert_dict: typing.Dict[str, typing.Any],
                                subject_suffix: str,
                                recipients: typing.Tuple[typing.Tuple[str, str]]) -> str:
    secrets = secrets_utils.get_secrets()

    with proxy_utils.set_env_http_proxy():
        account = exchange_utils.setup_exchange_account(secrets["proxy"]["username"],
                                                        secrets["proxy"]["password"], )

        # Forming email message
        email_subject = f"{ALERT_EMAIL_SUBJECT_PREFIX} - {subject_suffix}"
        email_request_id = str(uuid.uuid4())
        email_date = pandas.Timestamp.now().isoformat()
        suggested_post = alert_dict[TWEET_COL]
        link_str = LINK_TEMPLATE.format(alert_id=alert_dict[ID_COL])

        # removing null fields and tweet col for email generation
        fields_to_delete = [TWEET_COL]
        for k, v in alert_dict.items():
            if pandas.isna(v):
                fields_to_delete += [k]

        for k in fields_to_delete:
            if k in alert_dict:
                del alert_dict[k]

        logging.debug(f"{email_subject=}, {email_request_id=}, {email_date=}")

        logging.debug("Forming email body")
        with open(RESOURCES_PATH / ALERT_EMAIL_TEMPLATE, 'r') as template_file:
            message_body = jinja2.Template(template_file.read()).render(
                email_subject=email_subject,
                recipients=[name for name, _ in recipients if name],
                alert_dict=alert_dict,
                post_text=suggested_post,
                email_focus=subject_suffix,
                request_id=email_request_id,
                iso8601_timestamp=email_date,
                bok_link=link_str,
            )

        logging.debug("Creating email")
        message = Message(account=account,
                          body=HTMLBody(message_body),
                          subject=email_subject,
                          to_recipients=[email for _, email in recipients],
                          reply_to=DS_REPLY_TO)

        message.attach(_get_logo_attachment())

        logging.debug("Sending email")
        message.send()

        return message_body


class ServiceAlertEmailer(ServiceAlertBroadcaster):
    def __init__(self, minio_write_name=SA_EMAIL_NAME):
        super().__init__(minio_write_name=minio_write_name)

    def send_alert_emails(self):
        for config, (_, alert_df) in zip(SA_EMAIL_CONFIGS,
                                         self._service_alerts_generator(SA_EMAIL_CONFIGS)):
            if config.additional_filter:
                alert_df = alert_df.query(config.additional_filter)

            if alert_df.empty:
                logging.warning(f"{config=} results in an empty set - skipping!")
                continue

            for alert_dict in alert_df.to_dict(orient="records"):
                email_filename = f"{alert_dict[ID_COL]}.html"

                logging.debug("Checking if email has already been sent...")
                email_check = list(minio_utils.list_objects_in_bucket(self.minio_write_name,
                                                                      minio_prefix_override=email_filename))

                if len(email_check) > 0:
                    logging.warning(f"Skipping {alert_dict[ID_COL]} - already sent!")
                    continue

                email_message = _form_and_send_alerts_email(alert_dict, config.subject_suffix, config.receivers)

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
