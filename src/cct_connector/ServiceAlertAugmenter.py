import base64
import contextlib
import functools
import json
import logging
import os
import pathlib
import sys
import tempfile
import time
import typing

import geopandas
from db_utils import minio_utils, proxy_utils
from geospatial_utils import mportal_utils
import pandas
import requests
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.service import Service
from seleniumwire import webdriver
import shapely.wkt
from tqdm.auto import tqdm

from cct_connector import ServiceAlertBase
from cct_connector import (
    FIXED_SA_NAME, AUGMENTED_SA_NAME, SERVICE_ALERTS_PREFIX,
    AUGMENTER_SALT,
    ID_COL, TWEET_COL, TOOT_COL, IMAGE_COL,
)

# Internal LLM consts
CPTGPT_GPU_ENDPOINT = "https://cptgpt.capetown.gov.za/api/v1/chat/completions"
GPU_DRAFTING_MODEL = "wizardlm-13b-q5-gguf"
CPTGPT_CPU_ENDPOINT = "https://datascience.capetown.gov.za/cptgpt/v2/v1/chat/completions"
CPU_DRAFTING_MODEL = "wizardlm-13b-q5"
DRAFT_LIMIT = 10
PROMPT_LENGTH_LIMIT = 2048
DRAFT_TIMEOUT = 1200

SERVICE_AREA_HASHTAGS = {
    "Water & Sanitation": "#WaterAndSanitation",
    "Electricity": "#Electricity",
    "Refuse": "#Refuse",
    "Drivers Licence Enquiries": "#DLE",
    "Motor Vehicle Registration": "#MVR",
    "Water Management": "#MeterManagement",
    "Events": "#Events",
    "City Health": "#CityHealth",
}

AREA_TYPE_EXCLUSION_SET = {'Driving Licence Testing Centre', }
AREA_LOOKUP = {
    "Official Planning Suburb": ("Official planning suburbs", "OFC_SBRB_NAME"),
    "Solid Waste Regional Service Area": ("Solid Waste service areas", "AREA_NAME"),
    'Citywide': ('City boundary', 'CITY_NAME'),
}
AREA_INFERENCE_THRESHOLD = 0.05

AREA_WEBDRIVER_PATH = "/usr/bin/geckodriver"
AREA_IMAGE_SALT = "2024-03-20T22:59"
AREA_IMAGE_BUCKET = f"{SERVICE_ALERTS_PREFIX}.maps"
AREA_IMAGE_FILENAME_TEMPLATE = "{area_type_str}_{area_str}_{salt_str}.png"
AREA_IMAGE_DIM = 600
AREA_IMAGE_DELAY = 5
AREA_IMAGE_ZOOM = 13


@functools.lru_cache()
def _load_gis_layer(area_type: str, layer_query: str or None = None):
    if area_type in AREA_LOOKUP:
        layer_name, _ = AREA_LOOKUP[area_type]
    else:
        layer_name = area_type

    logging.debug(f"{area_type=}, {layer_name=}")
    layer_gdf = mportal_utils.load_sdc_datasets(layer_name, return_gdf=True)

    return layer_gdf.query(layer_query) if layer_query else layer_gdf


def _cptgpt_call_wrapper(message_dict: typing.Dict, http_session: requests.Session,
                         max_post_length: int) -> str or None:
    system_prompt = (
        f'Please reason step by step to draft {max_post_length} or less character social media posts about potential '
        'City of Cape Town service outage or update, using the details in provided JSON objects. '
        'The "service_area" field refers to the responsible department. '
        'Prioritise the location, date and time information '
        '- you don\'t have to mention all of the more technical details.'
        'Encourage the use of the "request_number" value when contacting the City. '
        'Adopt a formal, but polite tone. '
        'Correct any obvious spelling errors to South African English. '
        'Only return the content of the post for the very last JSON given.'
    )

    endpoint = CPTGPT_GPU_ENDPOINT
    params = {
        "model": GPU_DRAFTING_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": (
                '{"service_area":"Electricity","title":"Cable stolen","description":"Cable stolen","area":"LWANDLE",'
                ' "location":"Noxolost&surr…","start_timestamp":"2023-09-21T09:00:00+02:00",'
                ' "forecast_end_timestamp":"2023-09-23T13:00:00+02:00","planned":false,"request_number":"9115677540"}'
            )},
            {"role": "assistant", "content": (
                "🔌⚠️ Electricity service outage in Lwandle, Noxolo st & surrounding areas. Cable stolen. Restoration "
                "expected by 1pm, 23 Sep. For more info, contact City with request number 9115677540"
            )},
            # {"role": "user", "content": (
            #     '{"service_area": "Water & Sanitation","title": "Water Outages",'
            #     '"description": "Replacing of Fire Hydrant","area": "Parklands","location": "Gie Road, Parklands",'
            #     '"start_timestamp": "2023-09-28T12:00:00+02:00",'
            #     '"forecast_end_timestamp": "2023-09-29T20:00:00+02:00","planned": true,"request_number":"9115690645"}'
            # )},
            # {"role": "assistant", "content": (
            #     "🚧Water Outages🚧\n📍Gie Road, Parklands\n⏰Sep 28, 12:00 PM - Sep 29, 8:00 PM\n"
            #     "Potential water outage while replacing Fire Hydrant. Please use request number 9115690645 when "
            #     "contacting the City"
            # )},
            {"role": "user", "content": (
                '{"service_area": "Water & Sanitation","title": "Burst Water Main",'
                '"description": "Water streaming down the road. Both sides of road affected. '
                'Motorists to exercise caution. Maintenance team to conduct repairs asap.",'
                '"area": "SONEIKE II","location": "PAUL KRUGER, SONEIKE II",'
                '"start_timestamp": "2024-03-28T14:32:00","forecast_end_timestamp": "2024-03-29T20:00:00",'
                '"planned": false,"request_number": "9116963417","subtitle": "Running Water"}'
            )},
            {"role": "assistant", "content": (
                "🚧Burst Water Main🚧\n📍Paul Kruger, Soneike\n⏰Mar 28, 14:32 PM - Mar 29, 8:00 PM\n"
                "Water running down both sides of the road. Motorists to exercise caution. Please use request number "
                "9116963417 when contacting the City"
            )},

            {"role": "user", "content": json.dumps(message_dict)},
        ],
        "temperature": 0.2
    }

    for i, entry in enumerate(params["messages"]):
        message = entry['content']

        # stripping the request number field out of the prompts - it just confuses the model
        if 'request_number' not in message_dict:
            message = message.replace(
                'Encourage the use of the "request_number" value when contacting the City.', ''
            ).replace(
                ',"request_number":"9115677540"', ""
            ).replace(
                'For more info, contact City with request number 9115677540',
                "For more info, contact the City on 0860 103 089"
            ).replace(
                ',"request_number":"9115690645"', ""
            ).replace(
                'Please use request number 9115690645 when contacting the City',
                "For more info, contact the City on 0860 103 089"
            )

        params["messages"][i]["content"] = message

    # Wrapping call in retry loop
    last_error = None
    response_text = ""
    for t in range(3):
        try:
            rough_token_count = (len(json.dumps(params)) // 4) * 1.2 + 256
            expected_response_tokens = int((max_post_length // 4) * 2)
            logging.debug(f"{rough_token_count=}, {expected_response_tokens=}")

            if (rough_token_count + expected_response_tokens) > PROMPT_LENGTH_LIMIT:
                logging.warning("No hope of returning a valid response, skipping!")
                return None

            logging.debug(f"{params=}")

            response = http_session.post(endpoint, json=params, timeout=DRAFT_TIMEOUT)
            response_data = response.json()
            logging.debug(f"{response_data=}")

            response_text = response_data['choices'][0]['message']['content']

            assert len(response_text) < max_post_length, "Text too long!"

            return response_text
        except AssertionError as e:
            params["messages"] = [
                {"role": "system", "content": (
                    f'You shorten posts for social media. Please reason step by step summarise the post that follows to '
                    f'no more than {max_post_length} characters. Summarise any long lists using words like multiple or '
                    'many. Only return the content of the final summarised post.'
                )},
                {"role": "user", "content": (
                    '🚮 Refuse collection delays in Woodlands, Waters, Sea Point, Claremont, Lansdowne, Garlandale, '
                    'Bellville Industrial, Bellrail, Bville CBD, Sanlamhof, Dunrobin, Stikland, Saxon Industrial, '
                    'Ravensmead, Parow Industrial, Parow Industria, Epping 2. Leave bin out until 21:00 if not '
                    'serviced. Take bin onto property & place out by 06:30 the following day.'
                )},
                {"role": "assistant", "content": (
                    '🚮Refuse collection delays affecting multiple areas🚮. Leave bin out until 21:00 if not serviced,'
                    'and then put out again by 06:30 the next day'
                )},
                {"role": "user", "content": (
                    '🚧 Planned Maintenance 🚧\n'
                    '📍Die Wingerd, Greenway Rise, Stuart\'s Hill, Martinville, Schapenberg, Hageland Estate, '
                    'Sea View Lake Estate, Cherrywood Gardens (Bizweni - Somerset West)\n'
                    '⏰Thursday, 21:00 - 04:00\n'
                    'Zero-pressure testing on the water supply network'
                )},
                {"role": "assistant", "content": (
                    '🚧 Planned Maintenance 🚧\n'
                    '📍Multiple Areas\n'
                    '⏰Thursday, 21:00 - 04:00\n'
                    'Zero-pressure testing on the water supply network'
                )},
                {"role": "user", "content": response_text},
            ]

            params["temperature"] += 0.2

            last_error = e

        except Exception as e:
            logging.debug(f"Got {e.__class__.__name__}: '{e}'")
            last_error = e
            delay = t * 10
            logging.debug(f"sleeping for {delay}...")

            if endpoint == CPTGPT_GPU_ENDPOINT:
                logging.debug("Falling back to CPU model...")
                endpoint = CPTGPT_CPU_ENDPOINT
                params["model"] = CPU_DRAFTING_MODEL

            time.sleep(delay)

    else:
        if isinstance(last_error, requests.exceptions.ReadTimeout) or isinstance(last_error, KeyError):
            logging.error("CPTGPT timing out or malformed response - bailing!")
            sys.exit(-1)

    logging.warning(f"Inference failed - last error:{last_error.__class__.__name__}: '{last_error}'")

    return None


@contextlib.contextmanager
def _get_selenium_driver() -> webdriver.Firefox:
    logging.debug("Setting up Selenium webdriver...")

    # Turning down some of the loggers
    service = Service(executable_path=AREA_WEBDRIVER_PATH)

    options = FirefoxOptions()
    options.add_argument("--headless")

    with proxy_utils.set_env_http_proxy():
        proxy_str = os.environ["HTTPS_PROXY"]

    wire_options = {
        'proxy': {
            'http': proxy_str,
            'https': proxy_str,
            'no_proxy': f'localhost,127.0.0.1'
        }
    }

    browser = webdriver.Firefox(service=service,
                                options=options,
                                seleniumwire_options=wire_options)
    browser.implicitly_wait(300)
    logging.debug("...Setup Selenium webdriver")

    # Quietening some loggers
    for name in logging.root.manager.loggerDict:
        if (name.startswith("urllib3") or name.startswith("selenium") or
                name.startswith("hpack") or name.startswith("server") or name.startswith("ssa")):
            logging.getLogger(name).setLevel(logging.WARNING)

    yield browser

    browser.quit()


def _generate_screenshot_of_area(area_gdf: geopandas.GeoDataFrame, area_filename: str) -> bool:
    m = area_gdf.explore(max_zoom=AREA_IMAGE_ZOOM, zoom_control=False)

    with tempfile.TemporaryDirectory() as temp_dir, _get_selenium_driver() as driver:
        driver.set_window_size(AREA_IMAGE_DIM, AREA_IMAGE_DIM)
        local_image_path = pathlib.Path(temp_dir) / area_filename
        local_html_path = str(local_image_path).replace(".png", ".html")
        m.save(local_html_path)
        logging.debug(f"Map saved to {local_html_path}")

        logging.debug(f"Loading map from {local_html_path}...")
        driver.get(f"file://{local_html_path}")
        time.sleep(AREA_IMAGE_DELAY)
        logging.debug(f"...Loaded map from {local_html_path}")
        driver.save_screenshot(local_image_path)
        logging.debug(f"Saved to {local_image_path}")

        logging.debug(f"Uploading to {AREA_IMAGE_BUCKET}")
        return minio_utils.file_to_minio(local_image_path, AREA_IMAGE_BUCKET)


def _generate_image_link(area_type: str, area: str, wkt_str: str) -> str:
    area_image_filename = AREA_IMAGE_FILENAME_TEMPLATE.format(
        salt_str=base64.b64encode(bytes(AREA_IMAGE_SALT, 'utf-8')).decode(),
        area_type_str=base64.b64encode(bytes(area_type, 'utf-8')).decode(),
        area_str=base64.b64encode(bytes(area, 'utf-8')).decode(),
    )
    logging.debug(f"{AREA_IMAGE_SALT=}, {area_type=}, {area=}, {area_image_filename=}")

    for _ in minio_utils.list_objects_in_bucket(AREA_IMAGE_BUCKET, minio_prefix_override=area_image_filename):
        logging.debug(f"Cache hit on '{area_image_filename}', proceeding without regenerating the image")
        return area_image_filename

    logging.debug(f"Cache miss on '{area_image_filename}', generating screenshot...")
    area_gdf = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(wkt_str)], crs="EPSG:4326")
    _generate_screenshot_of_area(area_gdf, area_image_filename)
    logging.debug(f"Screenshot generate for '{area_image_filename}'")

    return area_image_filename


class ServiceAlertAugmenter(ServiceAlertBase.ServiceAlertsBase):
    def __init__(self, minio_read_name=FIXED_SA_NAME, minio_write_name=AUGMENTED_SA_NAME):
        super().__init__(None, None, minio_utils.DataClassification.LAKE,
                         minio_read_name=minio_read_name, minio_write_name=minio_write_name,
                         use_cached_values=True, stage_cache_salt=AUGMENTER_SALT, index_col=ID_COL)

        # all data is reverse sorted
        self.data = self.get_data_from_minio().sort_values(by=['publish_date'], ascending=False)

        # if there aren't new values, take some undrafted ones from the cache
        less_than_limit = DRAFT_LIMIT - self.data.shape[0]
        self.old_data = False
        # guard parameters
        if self.use_cached_values and (
                # detecting if cache values should be pulled in for back filling
                (self.cache_data[TWEET_COL].isna().any() or self.cache_data[TOOT_COL].isna().any()) and
                less_than_limit > 0
        ):
            logging.debug(f"Adding {less_than_limit} entries from cache into main data")
            moving_from_cache = self.cache_data.loc[
                self.cache_data[TWEET_COL].isna() | self.cache_data[TOOT_COL].isna(),
                self.data.columns
            ].sort_values(by=['publish_date']).tail(less_than_limit * 2).pipe(
                lambda df: df.sample(min([df.shape[0], less_than_limit]))
            )

            self.data = pandas.concat([
                self.data,
                moving_from_cache
            ])

            self.cache_data = self.cache_data.drop(moving_from_cache.index)
            self.old_data = True

    def add_social_media_posts(self, post_size_limit=280, social_media_col=TWEET_COL):
        source_data = self.data.copy()

        if self.old_data:
            source_data = source_data.head(DRAFT_LIMIT)
            logging.debug(f"Old data present, truncating to {DRAFT_LIMIT} values...")

        source_index = source_data.index.values
        source_data = source_data.drop(
            # Removing fields that often confuse the LLM
            columns=[
                c for c in
                (ID_COL, 'InputChecksum',
                 'publish_date', 'effective_date', 'expiry_date',
                 'notification_number',
                 'status',
                 'area_type', 'geospatial_footprint', IMAGE_COL,
                 TWEET_COL, TOOT_COL,
                 )
                if c in source_data.columns
            ]
        )

        # converting the timezone values to SAST
        for ts in ("start_timestamp", "forecast_end_timestamp"):
            source_data[ts] = source_data[ts].dt.tz_convert("+02:00")
            source_data[ts] = source_data[ts].dt.strftime("%Y-%m-%dT%H:%M:%S")

        json_dicts = source_data.to_dict(orient='records')

        with requests.Session() as session:
            for record_index, record in zip(source_index, tqdm(json_dicts)):
                logging.debug(f"Processing: {record_index} - {record}")

                # Removing any null keys
                keys_to_delete = [
                    k for k, v in record.items()
                    if pandas.isna(v)
                ]
                for field in keys_to_delete:
                    del record[field]

                if len(record.keys()) == 0:
                    logging.warning("Empty record, skipping!")
                    continue

                # ToDo use LLM to summarise any excessively long fields

                resp = _cptgpt_call_wrapper(record, session, post_size_limit)

                self.data.loc[record_index, social_media_col] = resp

    def add_social_media_posts_with_hashtags(self, source_col=TWEET_COL, destination_col=TOOT_COL):
        if source_col in self.data.columns and self.data.loc[self.data[TWEET_COL].notna(), source_col].notna().any():
            # NB this is a bit of a hack - this sort of thing should be left to the downstream consumer
            self.data[destination_col] = self.data.loc[self.data[TWEET_COL].notna(), source_col].copy()

            self.data[destination_col] = (
                    self.data[destination_col] + "\n" +
                    self.data["service_area"].map(SERVICE_AREA_HASHTAGS) + " #CapeTown"
            )
        else:
            logging.warning(f"Skipping because '{source_col}' is not in the data!")
            logging.debug(f"{self.data.columns}")

    def lookup_geospatial_footprint(self):
        logging.debug("Forming geospatial value lookup")
        area_type_spatial_lookup = {
            val: _load_gis_layer(val).set_index(AREA_LOOKUP[val][1])["WKT"].to_dict()
            for val in self.data["area_type"].unique()
            if val is not None and val not in AREA_TYPE_EXCLUSION_SET
        }

        footprint_lookup = self.data.query(
            "area_type.notna() and ~area_type.isin(@AREA_TYPE_EXCLUSION_SET)"
        ).apply(
            lambda row: (
                area_type_spatial_lookup[row["area_type"]][row["area"]]
                if row["area"] in area_type_spatial_lookup[row["area_type"]] else None
            ),
            axis=1
        ).dropna().astype(str)

        if not footprint_lookup.empty:
            self.data["geospatial_footprint"] = footprint_lookup

    def lookup_geospatial_image_link(self):
        if "geospatial_footprint" in self.data.columns:
            image_filename_lookup = self.data.query(
                "geospatial_footprint.notna()"
            ).apply(
                lambda row: _generate_image_link(row['area_type'], row['area'], row['geospatial_footprint']),
                axis=1
            )

            if not image_filename_lookup.empty:
                self.data[IMAGE_COL] = image_filename_lookup

    def infer_area(self, layer_name: str, layer_col: str, data_col_name: str, layer_query: str or None = None):
        layer_gdf = _load_gis_layer(layer_name, layer_query)[[layer_col, "WKT"]].assign(
            # getting total area before intersect operation
            area=lambda gdf: gdf.geometry.area
        )

        area_type_spatial_lookup = {
            val: _load_gis_layer(val).overlay(
                # using geospatial intersect to determine the overlap between the set area
                layer_gdf
            ).assign(
                # working out proportional area of the intersection
                prop_area=lambda gdf: gdf.geometry.area / gdf["area"]
            ).query(
                # applying the threshold - only count the intersection if it exceeds the threshold
                "prop_area > @AREA_INFERENCE_THRESHOLD"
            ).groupby(AREA_LOOKUP[val][1]).apply(
                # grouping back to a single entry per area
                lambda group_df: group_df[layer_col].values.astype(str)
            ).to_dict()
            for val in self.data["area_type"].unique()
            if val is not None and val not in AREA_TYPE_EXCLUSION_SET and AREA_LOOKUP[val][0] != layer_name
        }

        valid_mask = (
            self.data["area_type"].apply(
                lambda val: (
                    # checking that the area type is even in our lookup
                        val in AREA_LOOKUP and
                        # checking that we're not trying to infer the area that matches the area_type
                        AREA_LOOKUP[val][0] != layer_name
                )
            )
        )

        area_lookup = self.data.loc[valid_mask].apply(
            lambda row: (
                area_type_spatial_lookup[row["area_type"]][row["area"]]
                if row["area"] in area_type_spatial_lookup[row["area_type"]] else None
            ),
            axis=1
        ).dropna()

        if not area_lookup.empty:
            self.data[data_col_name] = area_lookup


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    logging.info("G[etting] data from Minio...")
    sa_augmenter = ServiceAlertAugmenter()
    logging.info("...G[ot] data from Minio")

    logging.info("Generat[ing] Tweets...")
    sa_augmenter.add_social_media_posts(280, TWEET_COL)
    logging.info("...Generat[ed] Tweets")

    logging.info("Generat[ing] Toots...")
    sa_augmenter.add_social_media_posts_with_hashtags()
    logging.info("...Generat[ed] Toots")

    logging.info("Look[ing] up Geospatial Footprint...")
    sa_augmenter.lookup_geospatial_footprint()
    logging.info("...Look[ed] up Geospatial Footprint")

    logging.info("Look[ing] up Image Filenames...")
    sa_augmenter.lookup_geospatial_image_link()
    logging.info("...Look[ed] up Image Filenames")

    logging.info("Inferr[ing] Suburbs...")
    sa_augmenter.infer_area("Official planning suburbs", "OFC_SBRB_NAME", "inferred_suburbs")
    logging.info("...Inferr[ed] Suburbs")

    logging.info("Inferr[ing] Wards...")
    sa_augmenter.infer_area("Wards", "WARD_NAME", "inferred_wards", "WARD_YEAR == 2021")
    logging.info("...Inferr[ed] Wards")

    logging.info("Wr[iting] to Minio...")
    sa_augmenter.write_data_to_minio(sa_augmenter.data)
    logging.info("...Wr[ote] to Minio")
