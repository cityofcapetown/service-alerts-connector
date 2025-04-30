import base64
import contextlib
import copy
import dataclasses
import enum
import functools
import hashlib
import itertools
import json
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
    TWEET_COL, FOOTPRINT_COL, SUMMARY_COL,
    SA_EMAIL_NAME, PHONE_NUMBER_LOOKUP_NAME, PHONE_NUMBER_LOOKUP_FILE,
    IMAGE_LINK_TEMPLATE
)
from cct_connector.ServiceAlertBroadcaster import ServiceAlertOutputFileConfig, ServiceAlertBroadcaster, ID_COL

DS_REPLY_TO = (
    "gordon.inggs@capetown.gov.za",
    "delyno.dutoit@capetown.gov.za",
    "henri.knoesen@capetown.gov.za",
    "kathryn.mcdermott@capetown.gov.za",
    "muhammed.ockards@capetown.gov.za",
)
RESOURCES_PATH = pathlib.Path(__file__).parent / ".." / "resources"
ALERT_EMAIL_TEMPLATE = "service_alert_tweet_emailer_template.html.jinja2"
CITY_LOGO_FILENAME = "rect_city_logo.png"
LINK_TEMPLATE = "https://ctapps.capetown.gov.za/sites/crhub/SitePages/ViewServiceAlert.aspx#?ID={alert_id}"
AREA_IMAGE_FILENAME = "area_image_filename.png"
EMAIL_LINK_TEMPLATE = "https://lake.capetown.gov.za/service-alerts.service-alerts-emails/{email_filename}"
FOOTPRINT_IMAGE_TEMPLATE = "https://service-alerts.cct-datascience.xyz/v1.3/footprint-map/{footprint_id}"

TURNIO_ENDPOINT = "https://whatsapp.turn.io/v1/messages"
TURNIO_NAMESPACE = "e737adae_bb1f_4551_a15b_e70bf7011942"
TURNIO_TEMPLATE = "city_alerts_v1"

class CommunicationPreference(enum.Enum):
    EMAIL = "email"
    WHATSAPP = "whatsapp"


@dataclasses.dataclass
class ServiceAlertEmailConfig(ServiceAlertOutputFileConfig):
    receivers: typing.Tuple[typing.Tuple[str or None, str], ...]
    email_focus: str
    additional_filter: str or typing.Callable or None

    comm_channel_preference: typing.Set[CommunicationPreference] = dataclasses.field(
        default_factory=lambda: {CommunicationPreference.EMAIL, CommunicationPreference.WHATSAPP}
    )

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


EMAIL_COLS = [ID_COL, "service_area", "title", "description", "status",
              "area_type", "area", "location",
              "inferred_wards", "inferred_suburbs", FOOTPRINT_COL,
              "start_timestamp", "forecast_end_timestamp",
              "planned", "request_number", TWEET_COL]


def _ward_curry_pot(ward_number: str) -> typing.Callable[[pandas.Series], bool]:
    # creating curried filter function
    def _ward_filter(row: pandas.Series) -> bool:
        return (row["inferred_wards"] is not None and
                ward_number in row["inferred_wards"] and
                row["area_type"] != "Citywide")

    return _ward_filter


def _service_area_curry_pot(service_area: str) -> typing.Callable[[pandas.Series], bool]:
    # creating curried filter function
    def _service_area_filter(row: pandas.Series) -> bool:
        return (row["service_area"] is not None and
                service_area == row["service_area"])

    return _service_area_filter


# ToDo move these to a YAML config
SA_EMAIL_CONFIGS = [
    # All Alerts
    # Debugging
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Gordon", "gordon.inggs@capetown.gov.za"),),
                            "all unplanned alerts",
                            None),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Gordon", "gordon.inggs@capetown.gov.za"),),
                            "all planned alerts",
                            None),
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Pia", "pia@turn.io"),),
                            "all unplanned alerts",
                            None, comm_channel_preference={CommunicationPreference.WHATSAPP}),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Pia", "pia@turn.io"),),
                            "all planned alerts",
                            None, comm_channel_preference={CommunicationPreference.WHATSAPP}),
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Wim", "wim.louw@capetown.gov.za"),),
                            "all unplanned alerts",
                            None),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Wim", "wim.louw@capetown.gov.za"),),
                            "all planned alerts",
                            None),
    # Social Media
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Social Media Team", "social.media@capetown.gov.za"),),
                            "all unplanned alerts",
                            None),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Social Media Team", "social.media@capetown.gov.za"),),
                            "all planned alerts",
                            None),
    # Digital Comms
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Justin", "justin.lawrence@capetown.gov.za"),),
                            "all unplanned alerts",
                            None),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Justin", "justin.lawrence@capetown.gov.za"),),
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
    # Water-specific
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Melissa", "Melissa.DeSousaAlves@capetown.gov.za"),
                             ("Water Dispatch Team", "Water.SanitationDispatch@capetown.gov.za"),),
                            "all planned water and sanitation work",
                            _service_area_curry_pot("Water & Sanitation")),
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Melissa", "Melissa.DeSousaAlves@capetown.gov.za"),
                             ("Water Dispatch Team", "Water.SanitationDispatch@capetown.gov.za"),),
                            "all unplanned water and sanitation alerts",
                            _service_area_curry_pot("Water & Sanitation")),
    # Wards
    # Ward 3
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Van Zyl", "annelize.vanZyl@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 3",
                            _ward_curry_pot("3")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Van Zyl", "annelize.vanZyl@capetown.gov.za"),),
                            "all planned works that might affect Ward 3",
                            _ward_curry_pot("3")),
    # Ward 16
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Barends", "ursula.barends@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 16",
                            _ward_curry_pot("16")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Barends", "ursula.barends@capetown.gov.za"),),
                            "all planned works that might affect Ward 16",
                            _ward_curry_pot("16")),
    # Ward 21
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Terblanche", "hendri.terblanche@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 21",
                            _ward_curry_pot("21")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Terblanche", "hendri.terblanche@capetown.gov.za"),),
                            "all planned works that might affect Ward 21",
                            _ward_curry_pot("21")),
    # Ward 31
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Ald Thompson", "theresa.thompson@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 31",
                            _ward_curry_pot("31")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Ald Thompson", "theresa.thompson@capetown.gov.za"),),
                            "all planned works that might affect Ward 31",
                            _ward_curry_pot("31")),
    # Ward 33
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Somdaka", "Lungisa.Somdaka@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 33",
                            _ward_curry_pot("33")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Somdaka", "Lungisa.Somdaka@capetown.gov.za"),),
                            "all planned works that might affect Ward 33",
                            _ward_curry_pot("33")),
    # Ward 34
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Gadeni", "Melikhaya.Gadeni@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 34",
                            _ward_curry_pot("34")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Gadeni", "Melikhaya.Gadeni@capetown.gov.za"),),
                            "all planned works that might affect Ward 34",
                            _ward_curry_pot("34")),
    # Ward 35
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Chitha", "Mboniswa.Chitha@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 35",
                            _ward_curry_pot("35")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Chitha", "Mboniswa.Chitha@capetown.gov.za"),),
                            "all planned works that might affect Ward 35",
                            _ward_curry_pot("35")),
    # Ward 36
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Ntshweza", "Nceba.Ntshweza@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 36",
                            _ward_curry_pot("36")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Ntshweza", "Nceba.Ntshweza@capetown.gov.za"),),
                            "all planned works that might affect Ward 36",
                            _ward_curry_pot("36")),
    # Ward 37
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Martin", "Lionel.Martin@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 37",
                            _ward_curry_pot("37")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Martin", "Lionel.Martin@capetown.gov.za"),),
                            "all planned works that might affect Ward 37",
                            _ward_curry_pot("37")),
    # Ward 38
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Zumana", "Suzanne.Zumana@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 38",
                            _ward_curry_pot("38")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Zumana", "Suzanne.Zumana@capetown.gov.za"),),
                            "all planned works that might affect Ward 38",
                            _ward_curry_pot("38")),
    # Ward 39
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Mjuza", "Thembinkosi.Mjuza@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 39",
                            _ward_curry_pot("39")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Mjuza", "Thembinkosi.Mjuza@capetown.gov.za"),),
                            "all planned works that might affect Ward 39",
                            _ward_curry_pot("39")),
    # Ward 40
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Ngcombolo", "bongani.ngcombolo@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 40",
                            _ward_curry_pot("40")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Ngcombolo", "bongani.ngcombolo@capetown.gov.za"),),
                            "all planned works that might affect Ward 40",
                            _ward_curry_pot("40")),
    # Ward 41
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Sonyoka", "Lindile.Sonyoka@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 41",
                            _ward_curry_pot("41")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Sonyoka", "Lindile.Sonyoka@capetown.gov.za"),),
                            "all planned works that might affect Ward 41",
                            _ward_curry_pot("41")),
    # Ward 42
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Esau", "Charles.Esau@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 42",
                            _ward_curry_pot("42")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Esau", "Charles.Esau@capetown.gov.za"),),
                            "all planned works that might affect Ward 42",
                            _ward_curry_pot("42")),
    # Ward 43
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Jansen", "EltonEnrique.Jansen@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 43",
                            _ward_curry_pot("43")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Jansen", "EltonEnrique.Jansen@capetown.gov.za"),),
                            "all planned works that might affect Ward 43",
                            _ward_curry_pot("43")),
    # Ward 44
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Moses", "Anthony.Moses@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 44",
                            _ward_curry_pot("44")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Moses", "Anthony.Moses@capetown.gov.za"),),
                            "all planned works that might affect Ward 44",
                            _ward_curry_pot("44")),
    # Ward 45
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Marr", "Mandy.Marr@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 45",
                            _ward_curry_pot("45")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Marr", "Mandy.Marr@capetown.gov.za"),),
                            "all planned works that might affect Ward 45",
                            _ward_curry_pot("45")),
    # Ward 47
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Ald van der Rheede", "Antonio.VanDerRheede@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 47",
                            _ward_curry_pot("47")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Ald van der Rheede", "Antonio.VanDerRheede@capetown.gov.za"),),
                            "all planned works that might affect Ward 47",
                            _ward_curry_pot("47")),
    # Ward 50
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr McKenzie", "angus.mckenzie@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 50",
                            _ward_curry_pot("50")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr McKenzie", "angus.mckenzie@capetown.gov.za"),),
                            "all planned works that might affect Ward 50",
                            _ward_curry_pot("50")),
    # Ward 52
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Nyamakazi", "Thembelani.Nyamakazi@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 52",
                            _ward_curry_pot("52")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Nyamakazi", "angus.mckenzie@capetown.gov.za"),),
                            "all planned works that might affect Ward 52",
                            _ward_curry_pot("52")),
    # Ward 54
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Jowell", "nicola.jowell@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 54",
                            _ward_curry_pot("54")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Jowell", "nicola.jowell@capetown.gov.za"),),
                            "all planned works that might affect Ward 54",
                            _ward_curry_pot("54")),
    # Ward 57
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Mohamed", "Yusuf.Mohamed@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 57",
                            _ward_curry_pot("57")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Mohamed", "Yusuf.Mohamed@capetown.gov.za"),),
                            "all planned works that might affect Ward 57",
                            _ward_curry_pot("57")),
    # Ward 58
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Hill", "Richard.Hill@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 58",
                            _ward_curry_pot("58")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Hill", "Richard.Hill@capetown.gov.za"),),
                            "all planned works that might affect Ward 58",
                            _ward_curry_pot("58")),
    # Ward 59
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Manuel", "Mikhail.Manuel@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 59",
                            _ward_curry_pot("59")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Manuel", "Mikhail.Manuel@capetown.gov.za"),),
                            "all planned works that might affect Ward 59",
                            _ward_curry_pot("59")),
    # Ward 60
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Kleinschmidt", "mark.kleinschmidt@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 60",
                            _ward_curry_pot("60")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Kleinschmidt", "mark.kleinschmidt@capetown.gov.za"),),
                            "all planned works that might affect Ward 60",
                            _ward_curry_pot("60")),
    # Ward 61
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Liell-Cock", "Simon.LiellCock@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 61",
                            _ward_curry_pot("61")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Liell-Cock", "Simon.LiellCock@capetown.gov.za"),),
                            "all planned works that might affect Ward 61",
                            _ward_curry_pot("61")),
    # Ward 62
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Langenhoven", "Emile.Langenhoven@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 62",
                            _ward_curry_pot("62")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Langenhoven", "Emile.Langenhoven@capetown.gov.za"),),
                            "all planned works that might affect Ward 62",
                            _ward_curry_pot("62")),
    # Ward 63
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Siebritz", "Carmen.Siebritz@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 63",
                            _ward_curry_pot("63")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Siebritz", "Carmen.Siebritz@capetown.gov.za"),),
                            "all planned works that might affect Ward 63",
                            _ward_curry_pot("63")),
    # Ward 64
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Sherry", "Izabel.Sherry@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 64",
                            _ward_curry_pot("64")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Sherry", "Izabel.Sherry@capetown.gov.za"),),
                            "all planned works that might affect Ward 64",
                            _ward_curry_pot("64")),
    # Ward 65
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Nelson", "Donovan.Nelson@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 65",
                            _ward_curry_pot("65")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Nelson", "Donovan.Nelson@capetown.gov.za"),),
                            "all planned works that might affect Ward 65",
                            _ward_curry_pot("65")),
    # Ward 66
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Akim", "william.akim@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 66",
                            _ward_curry_pot("66")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Akim", "william.akim@capetown.gov.za"),),
                            "all planned works that might affect Ward 66",
                            _ward_curry_pot("66")),
    # Ward 67
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Gordon", "Gerry.Gordon@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 67",
                            _ward_curry_pot("67")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Gordon", "Gerry.Gordon@capetown.gov.za"),),
                            "all planned works that might affect Ward 67",
                            _ward_curry_pot("67")),
    # Ward 68
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Petersen", "marita.petersen@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 68",
                            _ward_curry_pot("68")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Petersen", "marita.petersen@capetown.gov.za"),),
                            "all planned works that might affect Ward 68",
                            _ward_curry_pot("68")),
    # Ward 69
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Francke", "Patricia.Francke@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 69",
                            _ward_curry_pot("69")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Francke", "Patricia.Francke@capetown.gov.za"),),
                            "all planned works that might affect Ward 69",
                            _ward_curry_pot("69")),
    # Ward 71
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Franklin", "Carolynne.Franklin@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 71",
                            _ward_curry_pot("71")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Franklin", "Carolynne.Franklin@capetown.gov.za"),),
                            "all planned works that might affect Ward 71",
                            _ward_curry_pot("71")),
    # Ward 72
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Southgate", "Kevin.Southgate@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 72",
                            _ward_curry_pot("72")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Southgate", "Kevin.Southgate@capetown.gov.za"),),
                            "all planned works that might affect Ward 72",
                            _ward_curry_pot("72")),
    # Ward 73
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Ald Andrews", "Eddie.Andrews@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 73",
                            _ward_curry_pot("73")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Ald Andrews", "Eddie.Andrews@capetown.gov.za"),),
                            "all planned works that might affect Ward 73",
                            _ward_curry_pot("73")),
    # Ward 74
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Quintas", "roberto.quintas@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 74",
                            _ward_curry_pot("74")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Quintas", "roberto.quintas@capetown.gov.za"),),
                            "all planned works that might affect Ward 74",
                            _ward_curry_pot("74")),
    # Ward 75
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Woodman", "joan.woodman@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 75",
                            _ward_curry_pot("75")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Woodman", "joan.woodman@capetown.gov.za"),),
                            "all planned works that might affect Ward 75",
                            _ward_curry_pot("75")),
    # Ward 76
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Plaatjies", "Avron.Plaatjies@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 76",
                            _ward_curry_pot("76")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Plaatjies", "Avron.Plaatjies@capetown.gov.za"),),
                            "all planned works that might affect Ward 76",
                            _ward_curry_pot("76")),
    # Ward 77
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Higham", "Francine.Higham@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 77",
                            _ward_curry_pot("77")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Higham", "Francine.Higham@capetown.gov.za"),),
                            "all planned works that might affect Ward 77",
                            _ward_curry_pot("77")),
    # Ward 78
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Timm", "Goawa.Timm@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 78",
                            _ward_curry_pot("78")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Timm", "Goawa.Timm@capetown.gov.za"),),
                            "all planned works that might affect Ward 78",
                            _ward_curry_pot("78")),
    # Ward 79
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Christians", "Daniel.Christians@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 79",
                            _ward_curry_pot("79")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Christians", "Daniel.Christians@capetown.gov.za"),),
                            "all planned works that might affect Ward 79",
                            _ward_curry_pot("79")),
    # Ward 80
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Payiya", "Bennet.Payiya@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 80",
                            _ward_curry_pot("80")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Payiya", "Bennet.Payiya@capetown.gov.za"),),
                            "all planned works that might affect Ward 80",
                            _ward_curry_pot("80")),
    # Ward 81
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Potts", "Ashley.Potts@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 81",
                            _ward_curry_pot("81")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Potts", "Ashley.Potts@capetown.gov.za"),),
                            "all planned works that might affect Ward 81",
                            _ward_curry_pot("81")),

    # Ward 82
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Harris", "Washiela.Harris@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 82",
                            _ward_curry_pot("82")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Harris", "Washiela.Harris@capetown.gov.za"),),
                            "all planned works that might affect Ward 82",
                            _ward_curry_pot("82")),

    # Ward 88
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Sophazi", "Zukisani.Sophazi@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 88",
                            _ward_curry_pot("88")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Sophazi", "Zukisani.Sophazi@capetown.gov.za"),),
                            "all planned works that might affect Ward 88",
                            _ward_curry_pot("88")),

    # Ward 90
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Simangweni", "Lukhanyo.Simangweni@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 90",
                            _ward_curry_pot("90")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Simangweni", "Lukhanyo.Simangweni@capetown.gov.za"),),
                            "all planned works that might affect Ward 90",
                            _ward_curry_pot("90")),

    # Ward 92
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Adonis", "Norman.Adonis@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 92",
                            _ward_curry_pot("92")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Adonis", "Norman.Adonis@capetown.gov.za"),),
                            "all planned works that might affect Ward 92",
                            _ward_curry_pot("92")),

    # Ward 99
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Mqina", "Lonwabo.Mqina@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 99",
                            _ward_curry_pot("99")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Mqina", "Lonwabo.Mqina@capetown.gov.za"),),
                            "all planned works that might affect Ward 99",
                            _ward_curry_pot("99")),

    # Ward 110
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Ald Rossouw", "shanen.rossouw@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 110",
                            _ward_curry_pot("110")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Ald Rossouw", "shanen.rossouw@capetown.gov.za"),),
                            "all planned works that might affect Ward 110",
                            _ward_curry_pot("110")),

    # Ward 115
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr McMahon", "Ian.McMahon@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 115",
                            _ward_curry_pot("115")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr McMahon", "Ian.McMahon@capetown.gov.za"),),
                            "all planned works that might affect Ward 115",
                            _ward_curry_pot("115")),

    # Ward 116
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Cllr Philander", "Solomon.Philander@capetown.gov.za"),),
                            "all unplanned alerts that might affect Ward 116",
                            _ward_curry_pot("116")),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Cllr Philander", "Solomon.Philander@capetown.gov.za"),),
                            "all planned works that might affect Ward 116",
                            _ward_curry_pot("116")),

    # Grassy Park
    ServiceAlertEmailConfig("current", False, "v1", EMAIL_COLS,
                            (("Rejane", "rejane.alexander@capetown.gov.za"),),
                            "all unplanned alerts that affect Grassy Park",
                            "(inferred_suburbs.astype('str').str.lower().str.contains('grassy\Wpark') or "
                            " area.astype('str').str.lower().str.contains('grassy\Wpark'))"),
    ServiceAlertEmailConfig("current", True, "v1", EMAIL_COLS,
                            (("Rejane", "rejane.alexander@capetown.gov.za"),),
                            "all planned works that affect Grassy Park",
                            "(inferred_suburbs.astype('str').str.lower().str.contains('grassy\Wpark') or "
                            " area.astype('str').str.lower().str.contains('grassy\Wpark'))"),

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
    email_dict = copy.deepcopy(alert_dict)

    with proxy_utils.set_env_http_proxy():
        account = exchange_utils.setup_exchange_account(exchange_email="data.science@capetown.gov.za")

        # Forming email message
        if email_dict.get("status", "Open") == "Open":
            email_subject = f"New Service Alert - {email_dict['title']} in {email_dict['area']}"
        else:
            email_subject = f"Updated Service Alert - {email_dict['title']} in {email_dict['area']}"

        email_request_id = str(uuid.uuid4())
        email_date = pandas.Timestamp.now().isoformat()
        suggested_post = email_dict[TWEET_COL]
        link_str = LINK_TEMPLATE.format(alert_id=email_dict[ID_COL])
        image_link_str = (
            IMAGE_LINK_TEMPLATE.format(image_filename=email_dict[FOOTPRINT_COL])
            if email_dict[FOOTPRINT_COL] is not None
            else None
        )

        # removing null fields and tweet col for email generation
        fields_to_delete = [TWEET_COL, FOOTPRINT_COL, SUMMARY_COL]
        for k, v in email_dict.items():
            if not isinstance(v, typing.Collection) and pandas.isna(v):
                fields_to_delete += [k]
            elif isinstance(v, typing.Collection) and all(map(pandas.isna, v)):
                fields_to_delete += [k]

        if email_dict["area_type"] == "Official Planning Suburb":
            fields_to_delete += ["inferred_suburbs"]
        elif email_dict["area_type"] == "Citywide":
            fields_to_delete += ["inferred_suburbs", "inferred_wards"]

        for k in fields_to_delete:
            if k in email_dict:
                del email_dict[k]

        # formatting array fields
        for k, v in email_dict.items():
            if isinstance(v, typing.Collection) and not isinstance(v, str):
                email_dict[k] = ", ".join(v)

        logging.debug(f"{email_subject=}, {email_request_id=}, {email_date=}")

        logging.debug("Forming email body")
        with open(RESOURCES_PATH / ALERT_EMAIL_TEMPLATE, 'r') as template_file:
            message_body = jinja2.Template(template_file.read()).render(
                email_subject=email_subject,
                recipients=[name for name, _ in recipients if name],
                alert_dict=email_dict,
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
        logging.debug(f"{message.to_recipients=}")
        logging.debug(f"{message.subject=}")

        # Attaching logo
        logo_path = RESOURCES_PATH / CITY_LOGO_FILENAME
        message.attach(_get_image_attachment(logo_path, CITY_LOGO_FILENAME))

        # Attaching area image
        if image_link_str:
            with tempfile.NamedTemporaryFile("wb") as image_temp_file:
                image_temp_file.write(http_session.get(image_link_str,
                                                       proxies={'http': None, 'https': None}).content)
                image_temp_file.flush()

                message.attach(_get_image_attachment(pathlib.Path(image_temp_file.name).absolute(),
                                                     AREA_IMAGE_FILENAME))

        logging.debug("Sending email")
        message.send()

        return message_body


@functools.lru_cache()
def _load_phone_number_lookup() -> typing.Dict[str, typing.List[str]]:
    with tempfile.TemporaryDirectory() as tempdir:
        phone_number_lookup_filename = pathlib.Path(tempdir) / PHONE_NUMBER_LOOKUP_FILE
        minio_utils.minio_to_file(str(phone_number_lookup_filename), PHONE_NUMBER_LOOKUP_NAME)

        with open(phone_number_lookup_filename) as phone_number_lookup_file:
            phone_number_lookup = json.load(phone_number_lookup_file)

    return phone_number_lookup


def _form_and_send_whatsapp_messages(alert_dict: typing.Dict[str, typing.Any],
                                     phone_numbers: typing.List[str],
                                     http_session: requests.Session) -> str:
    secrets = secrets_utils.get_secrets()
    bearer_token = secrets["turnio"]["bearer_token"]
    auth_header = {"Authorization": f"Bearer {bearer_token}"}

    update_str = "a new" if alert_dict.get("status", "Open") == "Open" else "an update on an existing"
    image_url = (
        FOOTPRINT_IMAGE_TEMPLATE.format(footprint_id=alert_dict[FOOTPRINT_COL]) if alert_dict.get(FOOTPRINT_COL,
                                                                                                  None) is not None
        else "https://resource.capetown.gov.za/Style%20Library/Images/coct-logo@2x.png"
    )
    summary_str = alert_dict[TWEET_COL].replace("\n", " | ").replace("    ", " ")

    whatsapp_dict = {
        "type": "template",
        "template": {
            "namespace": TURNIO_NAMESPACE,
            "name": "coct_alerts_v2",
            "language": {
                "code": "en", "policy": "deterministic"},
            "components": [
                {"type": "header",
                 "parameters": [
                     {"type": "image",
                      "image": {
                          "link": image_url,
                      }}
                 ]},
                {"type": "body",
                 "parameters": [
                     {"type": "text",
                      "text": update_str},
                     {"type": "text",
                      "text": summary_str},
                 ]}
            ]
        }
    }

    for phone_number in phone_numbers:
        whatsapp_dict["to"] = phone_number

        resp = http_session.post(TURNIO_ENDPOINT,
                                 json=whatsapp_dict, headers=auth_header)
        resp.raise_for_status()

    return summary_str


class ServiceAlertEmailer(ServiceAlertBroadcaster):
    def __init__(self, minio_write_name=SA_EMAIL_NAME):
        super().__init__(minio_write_name=minio_write_name)

    def _config_alert_dict_generator(self, comms_preference_value: CommunicationPreference or None =None):
        for config, (*_, alert_df) in zip(SA_EMAIL_CONFIGS,
                                          self._service_alerts_generator(SA_EMAIL_CONFIGS)):
            config_hash = hashlib.sha256(str.encode(str(config.receivers) +
                                                    str(config.email_focus))).hexdigest()
            if alert_df.empty:
                logging.warning(f"Nothing more to do for {config=}, skipping!")
                continue

            if comms_preference_value and comms_preference_value not in config.comm_channel_preference:
                logging.warning(f"{comms_preference_value} not in {config.comm_channel_preference=}, skipping!")
                continue

            alert_df = config.apply_additional_filter(alert_df)

            for alert_dict in alert_df.to_dict(orient="records"):
                lower_status = alert_dict['status'].lower().replace(" ", "-")
                yield config_hash, config, alert_dict, lower_status

    def _in_cache(self, *args) -> bool:
        for mfn in args:
            logging.debug(f"Checking for {mfn} in cache")
            for fn in minio_utils.list_objects_in_bucket(self.minio_write_name,
                                                         minio_prefix_override=mfn):
                logging.debug(f"{fn} exists!")
                return True

        return False

    def _update_cache(self, content, filename, prefix_override=None):
        with tempfile.TemporaryDirectory() as tempdir:
            local_path = pathlib.Path(tempdir) / filename
            with open(local_path, "w") as local_file:
                local_file.write(content)

            minio_utils.file_to_minio(local_path, self.minio_write_name,
                                      filename_prefix_override=prefix_override)

    def send_alert_emails(self):
        with proxy_utils.setup_http_session() as http:
            for config_hash, config, alert_dict, lower_status in self._config_alert_dict_generator(CommunicationPreference.EMAIL):
                even_more_legacy_email_filename = f"{config_hash}_{alert_dict[ID_COL]}.html"
                legacy_email_filename = f"{config_hash}_{lower_status}_{alert_dict[ID_COL]}.html"
                # moving to same filename, but under a hashed prefix
                email_filename = f"{lower_status}_{alert_dict[ID_COL]}.html"

                if alert_dict[TWEET_COL] is None:
                    logging.warning(f"Empty post - {alert_dict[ID_COL]}")

                logging.debug("Checking if email has already been sent...")
                if not self._in_cache(even_more_legacy_email_filename, legacy_email_filename,
                                      f"{config_hash}/{email_filename}"):
                    logging.debug(f"Sending {alert_dict[ID_COL]}")
                    email_message = _form_and_send_alerts_email(alert_dict, config.email_focus, email_filename,
                                                                config.receivers,
                                                                http)

                    logging.debug("Backing up email")
                    self._update_cache(email_message, email_filename,
                                       prefix_override=config_hash + "/")

    def send_alert_whatsapps(self):
        phone_number_dict = _load_phone_number_lookup()

        with proxy_utils.setup_http_session() as http:
            for config_hash, config, alert_dict, lower_status in self._config_alert_dict_generator(CommunicationPreference.WHATSAPP):

                if alert_dict[TWEET_COL] is None:
                    logging.warning(f"Empty post - {alert_dict[ID_COL]}, skipping!")
                    continue

                # Iterating over receivers, and sending whatsapps
                for (_, email_address) in config.receivers:
                    whatsapp_filename = f"{lower_status}_{alert_dict[ID_COL]}_{base64.b64encode(email_address.encode()).decode()}.txt"

                    if not self._in_cache(f"{config_hash}/{whatsapp_filename}") and email_address in phone_number_dict:
                        logging.debug(f"Found phone number(s) for {email_address}, sending...")
                        whatsapp_message = _form_and_send_whatsapp_messages(alert_dict,
                                                                            phone_number_dict[email_address], http)

                        logging.debug("Backing up whatsapp")
                        self._update_cache(whatsapp_message, whatsapp_filename,
                                           prefix_override=config_hash + "/")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    logging.info("G[etting] data from Minio...")
    sa_emailer = ServiceAlertEmailer()
    logging.info("...G[ot] data from Minio")

    logging.info("Sen[ding] emails...")
    sa_emailer.send_alert_emails()
    logging.info("...Sen[t] emails")

    logging.info("Sen[ding] whatsapps...")
    sa_emailer.send_alert_whatsapps()
    logging.info("...Sen[t] whatsapps")
