import base64
import datetime
import functools
import json
import logging
import pathlib
import random
import tempfile
import time
import typing

from db_utils import minio_utils, proxy_utils, secrets_utils

from cct_connector import (
    ID_COL, TWEET_COL, FOOTPRINT_COL,
    PHONE_NUMBER_LOOKUP_NAME, PHONE_NUMBER_LOOKUP_FILE,
    SA_WA_NAME
)
from cct_connector.ServiceAlertEmailer import ServiceAlertEmailer, CommunicationPreference

FOOTPRINT_IMAGE_TEMPLATE = "https://service-alerts.cct-datascience.xyz/v1.3/footprint-map/{footprint_id}"

TURNIO_MESSAGES_ENDPOINT = "https://whatsapp.turn.io/v1/messages"
TURNIO_MEDIA_ENDPOINT = "https://whatsapp.turn.io/v1/media"
TURNIO_CONTACTS_ENDPOINT = "https://whatsapp.turn.io/v1/contacts"
TURNIO_NAMESPACE = "e737adae_bb1f_4551_a15b_e70bf7011942"
TURNIO_TEMPLATE = "secondalert_coct"


@functools.lru_cache()
def _load_phone_number_lookup() -> typing.Dict[str, typing.List[str]]:
    with tempfile.TemporaryDirectory() as tempdir:
        phone_number_lookup_filename = pathlib.Path(tempdir) / PHONE_NUMBER_LOOKUP_FILE
        minio_utils.minio_to_file(str(phone_number_lookup_filename), PHONE_NUMBER_LOOKUP_NAME)

        with open(phone_number_lookup_filename) as phone_number_lookup_file:
            phone_number_lookup = json.load(phone_number_lookup_file)

    return phone_number_lookup


class ServiceAlertWhatsapper(ServiceAlertEmailer):
    def __init__(self, minio_write_name=SA_WA_NAME):
        super().__init__(minio_write_name=minio_write_name)

        self.http_session = proxy_utils.setup_http_session()

        secrets = secrets_utils.get_secrets()
        self.auth_token = secrets["turnio"]["bearer_token"]

        # used to increment cache when a field changes
        self.cache_counter = 0

    @functools.lru_cache()
    def _get_profile_fields(self, whatsapp_id: str, cache_counter: int):
        # Getting profile info
        resp = self.http_session.get(TURNIO_CONTACTS_ENDPOINT + f"/{whatsapp_id}/profile",
                                     headers={"Authorization": f"Bearer {self.auth_token}",
                                              "Accept": "application/vnd.v1+json"})
        resp.raise_for_status()
        profile_fields = resp.json()['fields']

        return profile_fields

    @functools.lru_cache()
    def _whatsapp_upload_image(self, image_url: str) -> str:
        logging.debug("Uploading image")
        image_resp = self.http_session.get(image_url)
        image_resp.raise_for_status()

        resp = self.http_session.post(TURNIO_MEDIA_ENDPOINT,
                                      headers={"Authorization": f"Bearer {self.auth_token}",
                                               "Content-Type": "image/png"},
                                      data=image_resp.content)
        resp.raise_for_status()
        media_id = resp.json()["media"][0]["id"]
        logging.debug("Uploaded image")

        return media_id

    def _get_prompt_count(self, whatsapp_id):
        profile_fields = self._get_profile_fields(whatsapp_id, self.cache_counter)
        prompt_message_count = profile_fields.get('prompt_message_count', 0)

        return prompt_message_count

    def _last_message_sent_more_than(self, whatsapp_id, threshold: int = 3):
        profile_fields = self._get_profile_fields(whatsapp_id, self.cache_counter)
        last_message_sent_at = profile_fields.get('last_message_sent_at', None)

        if last_message_sent_at is None:
            return False
        else:
            last_message_sent_at = datetime.datetime.strptime(last_message_sent_at,
                                                              "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(datetime.timezone.utc)
            time_since_last_sent = datetime.datetime.now(datetime.timezone.utc) - last_message_sent_at

            return time_since_last_sent > datetime.timedelta(hours=threshold)


    def _increment_prompt_count(self, whatsapp_id: str):
        profile_fields = self._get_profile_fields(whatsapp_id, self.cache_counter)
        prompt_message_count = profile_fields.get('prompt_message_count', 0)

        logging.debug(f"Incrementing prompt count for {whatsapp_id=} ({prompt_message_count=})")
        resp = self.http_session.patch(TURNIO_CONTACTS_ENDPOINT + f"/{whatsapp_id}/profile",
                                       headers={"Authorization": f"Bearer {self.auth_token}",
                                                "Accept": "application/vnd.v1+json"},
                                       json={"prompt_message_count": prompt_message_count + 1})
        resp.raise_for_status()

        self.cache_counter += 1

    def _reset_prompt_message_count(self, whatsapp_id):
        logging.debug(f"Resetting prompt count for {whatsapp_id=}")
        resp = self.http_session.patch(TURNIO_CONTACTS_ENDPOINT + f"/{whatsapp_id}/profile",
                                       headers={"Authorization": f"Bearer {self.auth_token}",
                                                "Accept": "application/vnd.v1+json"},
                                       json={"prompt_message_count": 0})
        resp.raise_for_status()

        self.cache_counter += 1

    @functools.lru_cache()
    def _active_session(self, whatsapp_id: str, time_limit: int = 24) -> bool:
        profile_fields = self._get_profile_fields(whatsapp_id, self.cache_counter)

        last_message_received = profile_fields.get('last_message_received_at', None)

        # short-circuiting - we've never received a message from them
        if last_message_received is None:
            return False
        else:
            last_message_received = datetime.datetime.strptime(last_message_received,
                                                               "%Y-%m-%dT%H:%M:%SZ").astimezone(datetime.timezone.utc)
            # Doing datetime calculations
            time_since_last_received = datetime.datetime.now(datetime.timezone.utc) - last_message_received

            # have we received something in the last 24 hours?
            return time_since_last_received < datetime.timedelta(hours=time_limit)

    @functools.lru_cache()
    def _opted_in(self, whatsapp_id: str) -> bool:
        profile_fields = self._get_profile_fields(whatsapp_id, self.cache_counter)

        return profile_fields.get('opted_in', False)

    def send_session_prompt_message(self, whatsapp_id) -> bool:
        logging.warning(f"No session exists for {whatsapp_id}, sending prompt to kick one off")
        message_whatsapp_dict = {
            "to": whatsapp_id,
            "type": "template",
            "template": {
                "namespace": TURNIO_NAMESPACE,
                "name": TURNIO_TEMPLATE,
                "language": {"code": "en", "policy": "deterministic"},
                "components": []
            }
        }

        # Sending the message!
        resp = self.http_session.post(TURNIO_MESSAGES_ENDPOINT,
                                      json=message_whatsapp_dict,
                                      headers={"Authorization": f"Bearer {self.auth_token}"})
        resp.raise_for_status()

    def form_and_send_alert_messages(self, alert_dict: typing.Dict[str, typing.Any],
                                     whatsapp_id: str) -> str or None:
        # Getting image ID for map image
        image_url = (
            FOOTPRINT_IMAGE_TEMPLATE.format(footprint_id=alert_dict[FOOTPRINT_COL]) if alert_dict.get(FOOTPRINT_COL,
                                                                                                      None) is not None
            else "https://resource.capetown.gov.za/Style%20Library/Images/coct-logo@2x.png"
        )
        image_id = self._whatsapp_upload_image(image_url)

        # Forming the service message params
        message_content = alert_dict[TWEET_COL]
        message_whatsapp_dict = {
            "to": whatsapp_id,
            "preview_url": False,
            "recipient_type": "individual",
            "type": "image",
            "image": {
                "id": image_id,
                "caption": message_content
            }
        }

        # Sending the message!
        resp = self.http_session.post(TURNIO_MESSAGES_ENDPOINT,
                                      json=message_whatsapp_dict,
                                      headers={"Authorization": f"Bearer {self.auth_token}"})
        resp.raise_for_status()

        return message_content

    def send_alert_whatsapps(self):
        phone_number_dict = _load_phone_number_lookup()

        for config_hash, config, alert_dict, lower_status in self._config_alert_dict_generator(CommunicationPreference.WHATSAPP):
            if alert_dict[TWEET_COL] is None:
                logging.warning(f"Empty post - {alert_dict[ID_COL]}, skipping!")
                continue

            # Iterating over receivers, and sending whatsapps
            for (_, email_address) in config.receivers:
                phone_numbers = phone_number_dict.get(email_address, [])
                for phone_number in phone_numbers:
                    whatsapp_filename = f"{lower_status}_{alert_dict[ID_COL]}_{base64.b64encode(phone_number.encode()).decode()}.txt"

                    # Early exit if this is already in the cache
                    cache_key = f"{config_hash}/{whatsapp_filename}"
                    in_cache = self._in_cache(cache_key)

                    if in_cache:
                        logging.debug(f"{whatsapp_filename} in cache, skipping")
                        continue

                    whatsapp_message = None

                    opted_in = self._opted_in(phone_number)
                    active_session = self._active_session(phone_number)
                    prompt_counts = self._get_prompt_count(phone_number)
                    # prompt threshold is 2 for opted in users, 1 for not
                    prompt_threshold = 2 if self._opted_in(phone_number) else 1
                    # wait 3 hours between prompts
                    last_message_more_than_3_hours = self._last_message_sent_more_than(phone_number)
                    # reset prompts every 24 hours
                    last_message_more_than_24_hours = self._last_message_sent_more_than(phone_number, 24)
                    logging.debug(
                        f"{phone_number=}, "
                        f"{opted_in=}, {active_session=}, {prompt_counts=}, {prompt_threshold=}, "
                        f"{last_message_more_than_3_hours=}, {last_message_more_than_24_hours=}"
                    )

                    # has this message been sent? is the number opted in? is there a session active?
                    if opted_in and active_session:
                        logging.debug(f"Sending {alert_dict[ID_COL]} to {phone_number}!")
                        whatsapp_message = self.form_and_send_alert_messages(alert_dict, phone_number)
                        self._reset_prompt_message_count(phone_number)
                    # not sent, no session, below the prompt threshold
                    elif not active_session and prompt_counts < prompt_threshold and last_message_more_than_3_hours:
                        logging.debug(f"Sending session start prompt to {phone_number}!")
                        self.send_session_prompt_message(phone_number)
                        self._increment_prompt_count(phone_number)
                    # no active session, last message was more than 3 hours
                    elif not active_session and prompt_counts == prompt_threshold and last_message_more_than_24_hours:
                        logging.debug(f"Resetting prompt count!")
                        self._reset_prompt_message_count(phone_number)

                    if whatsapp_message:
                        logging.debug("Backing up whatsapp")
                        self._update_cache(whatsapp_message, whatsapp_filename,
                                           prefix_override=config_hash + "/")

                    # Adding back-off with some jitter
                    time.sleep(0.1 + 0.1 * random.random())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    logging.info("G[etting] data from Minio...")
    sa_whatsapper = ServiceAlertWhatsapper()
    logging.info("...G[ot] data from Minio")

    logging.info("Sen[ding] whatsapps...")
    sa_whatsapper.send_alert_whatsapps()
    logging.info("...Sen[t] whatsapps")
