import base64
import contextlib
import functools
import hashlib
import json
import logging
import os
import pathlib
import pprint
import sys
import tempfile
import time
import typing

import geopandas
from db_utils import minio_utils, proxy_utils, secrets_utils
from geopy.geocoders import Nominatim
from geospatial_utils import mportal_utils
import Levenshtein
import pandas
import requests
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.service import Service
from seleniumwire import webdriver
import shapely
import shapely.geometry
import shapely.wkt
from tqdm.auto import tqdm

from cct_connector import ServiceAlertBase
from cct_connector import (
    FIXED_SA_NAME, AUGMENTED_SA_NAME, SERVICE_ALERTS_PREFIX,
    AUGMENTER_SALT,
    ID_COL, TWEET_COL, TOOT_COL, IMAGE_COL, GEOSPATIAL_COL,
)

# Internal LLM consts
PRIMARY_GPT_ENDPOINT = "https://api.openai.com/v1/chat/completions"
PRIMARY_DRAFTING_MODEL = "gpt-3.5-turbo-16k"
FALLBACK_ENDPOINT = "https://datascience.capetown.gov.za/cptgpt-dev/v1/chat/completions"
FALLBACK_DRAFTING_MODEL = "llama3-8b-it-q5"
DRAFT_LIMIT = 10
PROMPT_LENGTH_LIMIT = 8192
DRAFT_TIMEOUT = 120

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

AREA_TYPE_EXCLUSION_SET = {'Driving Licence Testing Centre', 'Driving License Testing Centre'}
AREA_LOOKUP = {
    "Official Plannig Suburb": ("Official planning suburbs", "OFC_SBRB_NAME"),
    "Official Planning Suburb": ("Official planning suburbs", "OFC_SBRB_NAME"),
    "Solid Waste Regional Service Area": ("Solid Waste service areas", "AREA_NAME"),
    'Citywide': ('City boundary', 'CITY_NAME'),
}
AREA_INFERENCE_THRESHOLD = 0.05

GEOCODER_DELAY = 2
GEOCODER_TIMEOUT = 5
LOCATION_BUFFER = 0.0001  # In degrees decimal. At Cape Town's Lat-Long, this is about 10m
AREA_BUFFER = 0.01  # In degrees decimal. At Cape Town's Lat-Long, this is about 10m

AREA_WEBDRIVER_PATH = "/usr/bin/geckodriver"
AREA_IMAGE_SALT = "2024-03-20T22:59"
AREA_IMAGE_BUCKET = f"{SERVICE_ALERTS_PREFIX}.maps"
AREA_IMAGE_FILENAME_TEMPLATE = "{area_type_str}_{area_str}_{salt_str}.png"
LOCATION_IMAGE_FILENAME_TEMPLATE = "{area_type_str}_{area_str}_{location_str}_{salt_str}.png"
AREA_IMAGE_DIM = 600
AREA_IMAGE_DELAY = 5
AREA_IMAGE_ZOOM = 16


@functools.lru_cache
def _load_gis_layer(area_type: str, layer_query: str or None = None):
    if area_type in AREA_LOOKUP:
        layer_name, _ = AREA_LOOKUP[area_type]
    else:
        layer_name = area_type

    logging.debug(f"{area_type=}, {layer_name=}")
    layer_gdf = mportal_utils.load_sdc_datasets(layer_name, return_gdf=True)

    return layer_gdf.query(layer_query) if layer_query else layer_gdf


@functools.lru_cache
def _load_geocoder() -> Nominatim:
    return Nominatim(user_agent="cct-service-alert-pipeline")


@functools.lru_cache
def _cached_geocoder_wrapper(address: str, geometry: str = 'wkt') -> typing.Dict:
    with proxy_utils.set_env_http_proxy():
        geocoder = _load_geocoder()
        # Nominatim requests no more than 1 call per second
        time.sleep(GEOCODER_DELAY)
        return geocoder.geocode(address, geometry=geometry, timeout=GEOCODER_TIMEOUT)


@functools.lru_cache
def _get_overture_street_data() -> geopandas.GeoDataFrame:
    with tempfile.NamedTemporaryFile(suffix=".geojson") as temp_data_file:
        minio_utils.minio_to_file(temp_data_file.name,
                                  AREA_IMAGE_BUCKET,
                                  minio_filename_override="streets-lookup/cct_combined_roads.geojson")
        return geopandas.read_file(temp_data_file.name)


def _geocode_location(address: str,
                      bounding_polygon: shapely.geometry.base) -> shapely.geometry.base or None:
    output_shape = None
    logging.debug(f"Attempting to geocode '{address=}'")

    # Doing lookups against static data
    if ',' not in address:
        # NB do the query after the function call, otherwise it triggers a fetch of the layer
        address_lower = address.lower()
        suburb = _load_gis_layer("Official Planning Suburb").query(
            f"OFC_SBRB_NAME.str.lower() == @address_lower"
        )
        if suburb.shape[0] == 1:
            logging.debug(f"Found {address} in suburbs layer")
            output_shape = suburb.iloc[0]["WKT"]
    elif 'ward' not in address.lower():
        # Doing lookup against Overture-derived street map data, but only for suburb-like addresses
        street_name, *_ = address.split(',')
        streets_lookup_gdf = _get_overture_street_data().assign(
            score=lambda gdf: gdf["street_name"].apply(
                lambda street_lookup_name: Levenshtein.distance(street_name, street_lookup_name)
            )
        ).pipe(
            # selecting those within a reasonable distance
            lambda gdf: gdf.loc[gdf["score"] <= 5]
        ).pipe(
            # selecting those that fall within the bounding polygon
            lambda gdf: gdf.loc[
                gdf["geometry"].intersects(bounding_polygon)
            ]
        ).sort_values(by="score", ascending=False)
        logging.debug(f"{streets_lookup_gdf.shape=}")
        if not streets_lookup_gdf.empty:
            logging.debug(f"Found {address} in streets lookup!")
            logging.debug(f"streets_lookup_gdf.sample(5)=\n{streets_lookup_gdf.sample(min([5,streets_lookup_gdf.shape[0]]))}")

            # handling the case where there are multiple streets in the area with the same name
            # (usually segments that have gotten split up)
            output_street = streets_lookup_gdf['street_name'].iloc[-1]
            output_shapes = streets_lookup_gdf.query("street_name == @output_street")['geometry']
            logging.debug(f"Combing {len(output_shapes)} street segments with the same name")

            output_shape = shapely.unary_union(list(output_shapes)).buffer(LOCATION_BUFFER)

    # Next, try Nominatim geocoder
    if output_shape is None:
        address_string = f"{address}, Cape Town"
        geocoded_location = _cached_geocoder_wrapper(address_string)

        if geocoded_location is not None and "POINT" not in geocoded_location.raw["geotext"]:
            # handling the general case where we get a polygon or linestring back
            # generating location shape with suitable buffer
            output_shape = shapely.wkt.loads(geocoded_location.raw["geotext"]).buffer(LOCATION_BUFFER)
        elif geocoded_location is not None and "POINT" in geocoded_location.raw["geotext"]:
            # handle point location - presume it's a suburb or something similar
            south_lat, north_lat, west_lon, east_lon = geocoded_location.raw["boundingbox"]
            output_shape = shapely.geometry.Polygon([
                (west_lon, south_lat),
                (east_lon, south_lat),
                (east_lon, north_lat),
                (west_lon, north_lat),
                (west_lon, south_lat)  # Closing the polygon
            ])

    if output_shape is not None and not shapely.is_valid(output_shape):
        logging.warning("Invalid shape!")
        output_shape = None

    # finally, checking the location intersects with our bounding polygon
    if shapely.intersects(output_shape, bounding_polygon):
        logging.debug(f"{output_shape=}")
        # clipping output shape to fall within bounding area specified
        output_shape = shapely.intersection(output_shape, bounding_polygon.buffer(AREA_BUFFER))
    elif output_shape is not None:
        logging.warning(
            "Geocoded location does **not** intersect with bounding polygon, rejecting this location"
        )
        output_shape = None

    return output_shape


def _cptgpt_location_call_wrapper(location_dict: typing.Dict, http_session: requests.Session) -> typing.List or None:
    endpoint = PRIMARY_GPT_ENDPOINT
    # ToDo move messages to standalone config file
    params = {
        "model": PRIMARY_DRAFTING_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You reason step by step to prepare a nested JSON array of strings for submission to an online geocoding service that is very sensitive to correct spelling and exact wording. My intention is to use this service to translate text descriptions of locations within the City of Cape Town into cartographic polygons of areas affected by service outages.\n\nYou will be provided with JSON objects that might contain a \"area\" field, that contain entries from a controlled list of wider areas in the City of Cape Town, but definitely a \"location\" field that contain one or more specific locations within that area, captured as free text. \n\nPlace each location described in a separate array. Also, remove any references to generic areas of the City (e.g. Southern Suburbs). Within each location array, give up to 5 suggestions for strings to be submitting to the geocoding service. \n\nFor the first suggestion, stay close to the location as given, but for the subsequent entries draw upon your knowledge of addresses in general, and Cape Town in particular, to suggest possible variations in both suffixes (e.g. St, Rd, Cres) and spelling.\n\nOnly provide the resulting array of arrays for the last object given."
            },
            {
                "role": "user",
                "content": "{\narea\": \"VOELVLEI\",\n\"location\": \"The Voëlvlei Water Treatment Plant will be shut down for standard routine maintenance to operational valves from 08:00 until 16:00 on Thursday 25 April 2024. It will impact the water supply to the reservoirs supplying Durbanville, Fisantekraal, Klipheuwel, Uitzicht, Pinehurst and Sonstraalhoogte in the Northern parts of the city. Residents in these areas are kindly requested to reduce their consumption during this period. The City is topping up its reservoirs prior to the shutdown\"\n}"
            },
            {
                "role": "assistant",
                "content": "[[\"Durbanville\"], [\"Fisantekraal\"], [\"Klipheuwel\"] , [\"Uitzicht\"], [\"Pinehurst\"], [\"Sonstraalhoogte\"]]"
            },
            {
                "role": "user",
                "content": "{\n\"area\": \"BELLVILLE CBD\",\n\"location\": \"Ridgeworth and Bloemhof\"\n}"
            },
            {
                "role": "assistant",
                "content": "[[\"Ridgeworth, Bellville CBD\", \"Ridgeworth, Bellville\"], [\"Bloemhof, Bellville CBD\", \"Bloemhof, Bellville\"]]"
            },
            {
                "role": "user",
                "content": "{    \n\"area\": \"MILNERTON\",\n\"location\": \"Milnerton area\"\n}"
            },
            {
                "role": "assistant",
                "content": "[[\"Milnerton\"]]"
            },
            {
                "role": "user",
                "content": "{\n\"area\": \"MAMRE\",\n\"location\": \"Lavender St & surr\"\n}"
            },
            {
                "role": "assistant",
                "content": "[[\"Lavender St, Mamre\", \"Lavender Cres, Mamre\", \"Lavender Rd, Mamre\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"HOUT BAY\", \"location\": \"Portion of Hout Bay Harbour Rd & Immediate Surr\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Hout Bay Harbour Rd, Hout Bay\", \"Harbour Rd, Hout Bay\", \"Hout Bay Harbour Way, Hout Bay\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"NEWLANDS\", \"location\": \"Almond Road\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Almond Rd, Newlands\", \"Almond St, Newlands\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"MANENBERG\", \"location\": \"Yusuf Dadoo Road\\\\n\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Yusuf Dadoo Rd, Manenberg\", \"Yusuf Dadoo St, Manenberg\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"PARKLANDS\", \"location\": \"7 Malborough Road\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Malborough Rd, Parklands\", \"Malborough St, Parklands\", \"Marlborough Rd, Parklands\", \"Malborough Cres, Parklands\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"KLIPKOP\", \"location\": \"Claredon street\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Claredon St, Klipkop\", \"Clarendon St, Klipkop\", \"Claredon Rd, Klipkop\", \"Claredon Ave, Klipkop\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"VREDEKLOOF\", \"location\": \"De Bron, Sheba, Sonte, Garderner, Tivan, Sylvia, Barry, De Tuin, Daniel, Lula, Wilfrank, James & Rebecca - Vredekloof - Brackenfell\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"De Bron, Vredekloof\"],[\"Sheba, Vredekloof\"],[\"Sonte, Vredekloof\"],[\"Garderner, Vredekloof\", \"Gardener, Vredekloof\"],[\"Tivan, Vredekloof\"],[\"Sylvia, Vredekloof\"],[\"Barry, Vredekloof\"],[\"De Tuin, Vredekloof\"],[\"Daniel, Vredekloof\"],[\"Lula, Vredekloof\"],[\"Wilfrank, Vredekloof\"],[\"James, Vredekloof\"],[\"Rebecca, Vredekloof\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"VRYGROND\", \"location\": \"Vrygronf Road & surr\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Vrygrond Rd, Vrygrond\", \"Vrygrond St, Vrygrond\", \"Vrygrond Ave, Vrygrond\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"KHAYELITSHA\", \"location\": \"NTLANDLOLO ROAD, ENKANINI, KHAYELITSHA\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Ntlandlolo Rd, Khayelitsha\", \"Ntlandlolo Rd, Khayelitsha\", \"Ntlandlolo St, Khayelitsha\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"LEONSDALE\", \"location\": \"24 16th\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"16th St, Leonsdale\", \"16th Ave, Leonsdale\", \"16th Rd, Leonsdale\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"KLIPKOP\", \"location\": \"Koekelenberg Street\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Koekelenberg St, Klipkop\", \"Koekelenberg Rd, Klipkop\", \"Koekelenburg St, Klipkop\", \"Koegelenberg St, Klipkop\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"SCOTTSDENE\", \"location\": \"CAVALLERIA CRESCENT, sCOTTSDENE, KRAAIFONTEIN\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Cavalleria Cres, Scottsdene\", \"Cavalaria Cres, Scottsdene\", \"Cavaleria Cres, Scottsdene\", \"Cavaleria St, Scottsdene\"\"]]"
            },
            {
                "role": "user",
                "content": "{\"location\": \"AREA EAST COLLECTIONS Backlog Suburbs in Progress: Delayed Suburbs: Tuesday, 23 April 2024 Comment\\\\n\\\\uf0b7 None\\\\n\\\\uf0b7 Makhaza\\\\n\\\\uf0b7 Roundhay\\\\n\\\\uf0b7 Jaqueshill\\\\n\\\\uf0b7 Berbago\\\\n\\\\uf0b7 Die Wingerd\\\\n\\\\uf0b7 Briza\\\\n\\\\uf0b7 Goedehoop\\\\n\\\\uf0b7 Bridgewater\\\\n\\\\uf0b7 Carey Park\\\\n\\\\uf0b7 Longdown Estate\\\\n\\\\uf0b7 Westridge\\\\n\\\\uf0b7 Somersetridge\\\\n\\\\uf0b7 Motor City\\\\nPlease leave bin out until 21:00\\\\nIf not serviced\\\\ntake bin onto property and place out by 06:30 the following day\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Makhaza\"],[\"Roundhay\"],[\"Jaqueshill\", \"Jacques Hill\"],[\"Berbago\"],[\"Die Wingerd\", \"Die Wingert\"],[\"Goedehoop\"], [\"Bridgewater\"], [\"Carey Park\"], [\"Longdown Estate\"], [\"Westridge\"], [\"Somersetridge\", \"Somerset Ridge\"], [\"Motor City\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"CONSTANTIA\", \"location\": \"Avenue Beauvias Street\\\\nConstantia\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Avenue Beauvias St, Constantia\", \"Avenue Beauvais St, Constantia\", \"Beauvias Ave, Constantia\", \"Avenue Beauvais, Constantia\"]]"
            },
            {
                "role": "user",
                "content": "{\"location\": \"AREA SOUTH COLLECTIONS Backlog Suburbs in Progress: Delayed Suburbs: Tuesday, 23 April 2024 Comment\\\\n\\\\uf0b7 None\\\\n\\\\uf0b7 Westridge\\\\n\\\\uf0b7 Grassy Park\\\\n\\\\uf0b7 Lower Crossroads\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Westridge\"],[\"Grassy Park\"],[\"Lower Crossroads\", \"Crossroads\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"ATHLONE\", \"location\": \"Telford, Lawrence road & surrounds\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Telford Rd, Athlone\", \"Telford St, Athlone\"], [\"Lawrence Rd, Athlone\", \"Lawrence St, Athlone\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"HOUT BAY\", \"location\": \"Victorskloof \\\\u2013 Hout Bay\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Victorskloof, Hout Bay\", \"Victorskloof, Houtbay\",]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"SONKRING\", \"location\": \"Corner Berk and Vredeveld\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Berk, Sonkring\", \"Berk St, Sonkring\", \"Berk Rd, Sonkring\"],[\"Vredeveld, Sonkring\", \"Vredeveld St, Sonkring\", \"Vredeveld Rd, Sonkring\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"GLENLILY\", \"location\": \"C/O Fairfield & Third Avenue\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Fairfield Ave, Glenlily\", \"Fairfield Rd, Glenlily\"],[\"Third Avenue, Glenlily\", \"3rd Ave, Glenlily\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"MOWBRAY\", \"location\": \"Strubens roads\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Strubens Rd, Mowbray\", \"Struben St, Mowbray\", \"Strubens Ave, Mowbray\"]]"
            },
            {
                "role": "user",
                "content": "{\"area\": \"RAVENSMEAD\", \"location\": \"Florida Street, between 6th avenue and 15th avenue\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Florida St, Ravensmead\", \"Florida Rd, Ravensmead\"],[\"6th Ave, Ravensmead\", \"6th St, Ravensmead\"],[\"15th Ave, Ravensmead\", \"15th St, Ravensmead\"]]"
            },
            {
                "role": "user",
                "content": "{\"location\": \"AREA EAST COLLECTIONS:\n Mfuleni\n Kuilsriver Trade\n Gaylee\n Greenfields\n Sonkring\n Steynrust\"}"
            },
            {
                "role": "assistant",
                "content": "[[\"Mfuleni\"], [\"Kuilsrivier Industria\", \"Kuilsrivier\",\"Kuils River\"], [\"Gaylee\"], [\"Greenfields\"],[\"Sonkring\"], [\"Steynrust\"]]"
            },
            {
                "role": "user",
                "content": json.dumps(location_dict)
            }
        ],
        "temperature": 0.2,
    }
    headers = {}

    # Wrapping call in retry loop
    last_error = None
    response_text = ""
    for t in range(3):
        try:
            logging.debug(f"{params=}")

            if "openai" in endpoint:
                secrets = secrets_utils.get_secrets()
                headers['Authorization'] = f"Bearer {secrets['openai_api_key']}"
            else:
                del headers['Authorization']

            response = http_session.post(endpoint, json=params, timeout=DRAFT_TIMEOUT, headers=headers)
            response_data = response.json()
            logging.debug(f"{response_data=}")

            response_text = response_data['choices'][0]['message']['content']
            response_json = json.loads(response_text)

            # correcting common misconstructions
            # first up, three layer arrays (should only be two)
            if isinstance(response_json, list) and len(response_json) == 1:
                if isinstance(response_json[0], list) and len(response_json[0]) == 1:
                    if isinstance(response_json[0][0], list):
                        logging.debug("Threefold nested array, unpacking one layer")
                        response_json = response_json[0]
            # then, single layer arrays
            elif isinstance(response_json, list) and len(response_json) >= 1 and all(
                    map(lambda val: isinstance(val, str), response_json)):
                logging.debug("Flat array of strings, adding another layer")
                response_json = [response_json]

            # checking response is the form that we expect
            assert isinstance(response_json, list), f"Expected a JSON array back, got '{response_text}'"
            assert all(map(lambda val: isinstance(val, list), response_json)), (
                f"Expected only arrays in the outer array, got '{response_text}'"
            )
            for json_array in response_json:
                assert all(map(lambda val: isinstance(val, str), json_array)), (
                    f"Expected only strings in the inner array, got '{json_array}'"
                )

            return response_json

        except (json.JSONDecodeError, AssertionError) as e:
            logging.debug(f"Got {e.__class__.__name__}: '{e}'")
            last_error = e

            if t == 0 and len(response_text) > 0:
                params["messages"] += [
                    {
                        "role": "assistant",
                        "content": response_text
                    },
                    {
                        "role": "user",
                        "content": "You responded with a malformed or incorrectly structured JSON array. "
                                   "Please fix this to only be nested JSON array(s) of the location(s) in the last JSON"
                                   " object I provided you with. Only return the corrected JSON array."
                    }
                ]
            else:
                params["temperature"] += 0.1

            delay = t * 10
            logging.debug(f"sleeping for {delay}s...")
            time.sleep(delay)

        except Exception as e:
            logging.debug(f"Got {e.__class__.__name__}: '{e}'")
            last_error = e
            delay = t * 10
            logging.debug(f"sleeping for {delay}...")

            if endpoint == PRIMARY_GPT_ENDPOINT:
                logging.debug("Falling back to alternative model...")
                endpoint = FALLBACK_ENDPOINT
                params["model"] = FALLBACK_DRAFTING_MODEL

            time.sleep(delay)

    else:
        if isinstance(last_error, requests.exceptions.ReadTimeout) or isinstance(last_error, KeyError):
            logging.error("CPTGPT timing out or malformed response - bailing!")
            sys.exit(-1)

    logging.warning(f"Inference failed - last error: {last_error.__class__.__name__}: '{last_error}'")

    return None


def _cptgpt_summarise_call_wrapper(message_dict: typing.Dict, http_session: requests.Session,
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

    endpoint = PRIMARY_GPT_ENDPOINT
    params = {
        "model": PRIMARY_DRAFTING_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    '{"service_area":"Electricity","title":"Cable stolen","description":"Cable stolen","area":"LWANDLE",'
                    ' "location":"Noxolo st&surr…","start_timestamp":"2023-09-21T09:00:00+02:00",'
                    ' "forecast_end_timestamp":"2023-09-23T13:00:00+02:00","planned":false,"request_number":"9115677540"}'
                )},
            {
                "role": "assistant",
                "content": (
                    "🔌⚠️ Electricity service outage in Lwandle, Noxolo st & surrounding areas. Cable stolen. Restoration "
                    "expected by 1pm, 23 Sep. For more info, contact City with request number 9115677540"
                )},
            {
                "role": "user",
                "content": (
                    '{"service_area": "Water & Sanitation","title": "Burst Water Main",'
                    '"description": "Water streaming down the road. Both sides of road affected. '
                    'Motorists to exercise caution. Maintenance team to conduct repairs asap.",'
                    '"area": "SONEIKE II","location": "PAUL KRUGER, SONEIKE II",'
                    '"start_timestamp": "2024-03-28T14:32:00","forecast_end_timestamp": "2024-03-29T20:00:00",'
                    '"planned": false,"request_number": "9116963417","subtitle": "Running Water"}'
                )},
            {
                "role": "assistant",
                "content": (
                    "🚧Burst Water Main🚧\n📍Paul Kruger, Soneike\n⏰Mar 28, 14:32 PM - Mar 29, 8:00 PM\n"
                    "Water running down both sides of the road. Motorists to exercise caution. Please use request number "
                    "9116963417 when contacting the City"
                )},
            {
                "role": "user",
                "content": (
                    "{\"service_area\": \"Electricity\", \"title\": \"Electrical Maintenance - "
                    "Kiosk Replacement/repair\", \"description\": \"Electricity supply affected in surrounding "
                    "area due to replacement of kiosk.\", \"area\": \"DURBANVILLE\", \"location\": \"Jagger St\", "
                    "\"start_timestamp\": \"2024-04-30T06:00:00.000Z\", "
                    "\"forecast_end_timestamp\": \"2024-04-30T14:00:00.000Z\", "
                    "\"planned\": true}"
                )},
            {
                "role": "assistant",
                "content": (
                    "🔌 Planned Electrical Maintenance in Durbanville\n"
                    "📍 Jagger St\n"
                    "⏰ Apr 30, 8am - 4pm\n"
                    "Electricity supply affected due to kiosk replacement. Please note that this is a planned "
                    "outage and we apologise for any inconvenience caused. For more info, contact the City on "
                    "0860 103 089"
                )
            },
            {"role": "user", "content": json.dumps(message_dict)},
        ],
        "temperature": 0.2,
        "frequency_penalty": 1,
    }
    headers = {}

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
        post_too_long = False
        post_just_one_char = False
        try:
            rough_token_count = (len(json.dumps(params)) // 4) * 1.2 + 256
            expected_response_tokens = int((max_post_length // 4) * 2)
            logging.debug(f"{rough_token_count=}, {expected_response_tokens=}")
            params["max_tokens"] = expected_response_tokens

            if (rough_token_count + expected_response_tokens) > PROMPT_LENGTH_LIMIT:
                logging.warning("No hope of returning a valid response, skipping!")
                return None

            logging.debug(f"{params=}")

            if "openai" in endpoint:
                secrets = secrets_utils.get_secrets()
                headers['Authorization'] = f"Bearer {secrets['openai_api_key']}"
            else:
                del headers['Authorization']

            response = http_session.post(endpoint, json=params, timeout=DRAFT_TIMEOUT, headers=headers)
            response_data = response.json()
            logging.debug(f"{response_data=}")

            response_text = response_data['choices'][0]['message']['content']
            post_too_long = len(response_text) > max_post_length
            assert not post_too_long, "Text too long!"

            post_just_one_char = len(set(response_text)) == 1
            assert not post_just_one_char, "Only one character returned!"

            return response_text
        except AssertionError as e:
            if post_too_long:
                params["messages"] = [
                    {"role": "system", "content": (
                        f'You shorten posts for social media. Please reason step by step summarise the post that follows to '
                        f'no more than {max_post_length} characters. Summarise any long lists using words like multiple or '
                        'many. Prioritise dates and area information over the causes of issues. Only return the content of '
                        'the final summarised post.'
                    )},
                    {"role": "user", "content": (
                        '🚮 Refuse collection delays in Woodlands, Waters, Sea Point, Claremont, Lansdowne, Garlandale, '
                        'Bellville Industrial, Bellrail, Bville CBD, Sanlamhof, Dunrobin, Stikland, Saxon Industrial, '
                        'Ravensmead, Parow Industrial, Parow Industria, Epping 2. Due to strike action. Leave bin out until'
                        ' 21:00 if not serviced. Take bin onto property & place out by 06:30 the following day.'
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

            if endpoint == PRIMARY_GPT_ENDPOINT:
                logging.debug("Falling back to alternative model...")
                endpoint = FALLBACK_ENDPOINT
                params["model"] = FALLBACK_DRAFTING_MODEL

            time.sleep(delay)

    else:
        if (
                isinstance(last_error, requests.exceptions.ReadTimeout) or
                isinstance(last_error, KeyError) or
                isinstance(last_error, requests.exceptions.ConnectionError)
        ):
            logging.error("CPTGPT timing out or malformed response - bailing!")
            sys.exit(-1)

    logging.warning(f"Inference failed - last error:{last_error.__class__.__name__}: '{last_error}'")

    return None


@contextlib.contextmanager
def _get_selenium_driver() -> webdriver.Firefox:
    logging.debug("Setting up Selenium webdriver...")

    # Turning down some of the loggers
    service = Service(executable_path=AREA_WEBDRIVER_PATH)
    # os.environ["TMPDIR"] = "/home/gordon/snap/firefox/common/tmp"
    # service = Service(executable_path="/home/gordon/Downloads/geckodriver")

    options = FirefoxOptions()
    options.add_argument("--headless")

    with proxy_utils.set_env_http_proxy():
        proxy_str = os.environ["HTTPS_PROXY"]

    wire_options = {
        'proxy': {
            'http': proxy_str,
            'https': proxy_str,
            'no_proxy': f'localhost,127.0.0.1,lake.capetown.gov.za'
        }
    }

    browser = webdriver.Firefox(service=service,
                                options=options,
                                seleniumwire_options=wire_options)
    browser.implicitly_wait(30)
    logging.debug("...Setup Selenium webdriver")

    # Quietening some loggers
    for name in logging.root.manager.loggerDict:
        if (name.startswith("urllib3") or name.startswith("selenium") or
                name.startswith("hpack") or name.startswith("server") or name.startswith("ssa")):
            logging.getLogger(name).setLevel(logging.WARNING)

    yield browser

    browser.quit()


def _generate_screenshot_of_area(area_gdf: geopandas.GeoDataFrame, area_filename: str) -> bool:
    m = area_gdf.explore(max_zoom=AREA_IMAGE_ZOOM, zoom_control=False,
                         attr="©OpenStreetMap Contributors; ©Overture Maps Foundation")

    logging.debug("Setting up temp dir and webdriver")
    with tempfile.TemporaryDirectory() as temp_dir, _get_selenium_driver() as driver:
        driver.set_window_size(AREA_IMAGE_DIM, AREA_IMAGE_DIM)
        # temp_dir = "/home/gordon/snap/firefox/common/tmp"
        local_image_path = pathlib.Path(temp_dir) / area_filename
        local_html_path = str(local_image_path).replace(".png", ".html")
        logging.debug(f"Saving map to {local_html_path}")
        m.save(local_html_path)
        logging.debug(f"Map saved to {local_html_path}")

        logging.debug(f"Loading map from {local_html_path}...")
        driver.get(f"file://{local_html_path}")
        time.sleep(AREA_IMAGE_DELAY)
        logging.debug(f"...Loaded map from {local_html_path}")
        driver.save_screenshot(str(local_image_path))
        logging.debug(f"Saved to {local_image_path}")

        logging.debug(f"Uploading to {AREA_IMAGE_BUCKET}")

        return minio_utils.file_to_minio(local_image_path, AREA_IMAGE_BUCKET)


def _generate_image_link(area_type: str, area: str, location: str or None, wkt_str: str) -> str:
    template_params = dict(
        salt_str=base64.b64encode(bytes(AREA_IMAGE_SALT, 'utf-8')).decode(),
        area_type_str=base64.b64encode(bytes(area_type, 'utf-8')).decode(),
        area_str=base64.b64encode(bytes(area, 'utf-8')).decode(),
    )
    template_str = AREA_IMAGE_FILENAME_TEMPLATE

    if pandas.notna(location):
        template_str = LOCATION_IMAGE_FILENAME_TEMPLATE
        template_params["location_str"] = base64.b64encode(bytes(location, 'utf-8')).decode()

    area_image_filename = template_str.format(**template_params)

    if len(area_image_filename) > 32:
        area_image_filename = hashlib.sha256(area_image_filename.encode()).hexdigest() + ".png"

    logging.debug(f"{AREA_IMAGE_SALT=}, {area_type=}, {area=}, {location=}, {area_image_filename=}")

    for _ in minio_utils.list_objects_in_bucket(AREA_IMAGE_BUCKET, minio_prefix_override=area_image_filename):
        logging.debug(f"Cache hit on '{area_image_filename}', proceeding without regenerating the image")
        return area_image_filename

    logging.debug(f"Cache miss on '{area_image_filename}', generating screenshot...")
    area_gdf = geopandas.GeoDataFrame(geometry=[shapely.wkt.loads(wkt_str)], crs="EPSG:4326")
    _generate_screenshot_of_area(area_gdf, area_image_filename)
    logging.debug(f"Screenshot generated for '{area_image_filename}'")

    return area_image_filename


def _lookup_area_geospatial_footprint(area_df: pandas.DataFrame) -> pandas.Series:
    logging.debug("Forming geospatial value lookup")
    area_type_spatial_lookup = {
        val: _load_gis_layer(val).assign(**{
            # turns out some of the GIS datasets have trailing spaces
            AREA_LOOKUP[val][1]: lambda df: df[AREA_LOOKUP[val][1]].str.strip()
        }).set_index(AREA_LOOKUP[val][1])["WKT"].to_dict()
        for val in area_df["area_type"].unique()
        if val is not None and val not in AREA_TYPE_EXCLUSION_SET
    }

    area_lookup_df = area_df.query(
        "area_type.notna() and ~area_type.isin(@AREA_TYPE_EXCLUSION_SET)"
    ).apply(
        lambda row: (
            # reducing precision to 6 decimal places
            shapely.wkt.dumps(
                area_type_spatial_lookup[row["area_type"]][row["area"]],
                rounding_precision=6
            )
            if row["area"] in area_type_spatial_lookup[row["area_type"]] else None
        ),
        axis=1
    ).dropna()
    logging.debug(f"{area_df.shape=}, {area_lookup_df.shape=}")

    return area_lookup_df


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
                 'area_type', GEOSPATIAL_COL, IMAGE_COL,
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

        with proxy_utils.setup_http_session() as session:
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

                resp = _cptgpt_summarise_call_wrapper(record, session, post_size_limit)

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

    def lookup_geospatial_image_link(self):
        if "geospatial_footprint" in self.data.columns:
            image_filename_lookup = self.data.query(
                "geospatial_footprint.notna()"
            ).apply(
                lambda row: _generate_image_link(row['area_type'], row['area'], row['location'],
                                                 row[GEOSPATIAL_COL]),
                axis=1
            )

            if not image_filename_lookup.empty:
                self.data[IMAGE_COL] = image_filename_lookup

    def infer_area(self, layer_name: str, layer_col: str, data_col_name: str, layer_query: str or None = None):
        if self.data.empty:
            logging.warning("No data, so skipping...")
            return
        elif "geospatial_footprint" not in self.data.columns:
            logging.warning("No geospatial data, so skipping...")
            return

        layer_gdf = _load_gis_layer(layer_name, layer_query)[[layer_col, "WKT"]].assign(
            layer_area=lambda gdf: gdf.geometry.area
        )

        # assembling locations of interest
        source_data = self.data.copy().dropna(subset="geospatial_footprint")
        data_locs = geopandas.GeoDataFrame({"index": source_data.index},
                                           index=source_data.index,
                                           geometry=source_data["geospatial_footprint"].apply(shapely.wkt.loads),
                                           crs="EPSG:4326").assign(
            data_area=lambda gdf: gdf.geometry.area
        )

        area_lookup = data_locs.overlay(
            layer_gdf
        ).assign(
            # working out proportional area of the intersection with areas being inferred
            layer_prop_area=lambda gdf: gdf.geometry.area / gdf["layer_area"],
            data_prop_area=lambda gdf: gdf.geometry.area / gdf["data_area"],
        ).query(
            # applying the threshold - only include the intersection if it exceeds the threshold
            "layer_prop_area > @AREA_INFERENCE_THRESHOLD or data_prop_area > @AREA_INFERENCE_THRESHOLD"
        ).groupby("index").apply(
            lambda group_df: list(group_df[layer_col].astype(str))
        )

        if not area_lookup.empty:
            self.data[data_col_name] = area_lookup

    def lookup_location_geospatial_footprint(self):
        if "area_type" not in self.data.columns or self.data["area_type"].isna().all():
            logging.warning("Area type not present in data, skipping geospatial lookups")
            return
        elif "area_type" in self.data.columns and self.data["area_type"].isin(AREA_TYPE_EXCLUSION_SET).all():
            logging.warning("Area type is present in data, but all of exclusion types, skipping geospatial lookups")
            return
        elif self.data.empty:
            logging.warning("Nothing to do here, skipping...")
            return

        source_data = self.data[["area_type", "area", "location"]].dropna(subset=["area_type"]).copy()
        source_index = source_data.index.values

        # loading up the layers we're going to use
        area_polygons = _lookup_area_geospatial_footprint(source_data[["area_type", "area"]])
        area_polygons = geopandas.GeoDataFrame(
            geometry=area_polygons.apply(shapely.wkt.loads),
            index=source_index
        )
        ward_polygons = _load_gis_layer("Wards", "WARD_YEAR == 2021")[
            ["WARD_NAME", "WKT"]
        ].set_index("WARD_NAME")

        json_dicts = source_data.to_dict(orient='records')
        with proxy_utils.setup_http_session() as session:
            for record_index, record in zip(source_index, tqdm(json_dicts)):
                if record["area_type"] in AREA_TYPE_EXCLUSION_SET:
                    logging.warning(f"Skipping {record_index} from geocoding because it is of an excluded area type!")
                    continue

                # todo implement cache lookup and skip, if possible
                # Getting list of locations via LLM
                llm_record = {
                    "area": record["area"],
                    "location": record["location"],
                }
                if record["area_type"] != "Official Planning Suburb":
                    del llm_record["area"]

                llm_locations = _cptgpt_location_call_wrapper(llm_record, session)
                logging.debug(f"{llm_locations=}")

                area_polygon = area_polygons.loc[record_index, 'geometry']
                if llm_locations is None or len(llm_locations) == 0:
                    logging.warning(f"Empty response from LLM for {record}, falling back to area polygon")
                    record_polygon = area_polygon
                elif area_polygon is None:
                    logging.warning(f"Can't look up a polygon for this area: {source_index}")
                else:
                    # Getting relevant geometries for this record
                    intersecting_wards = ward_polygons.loc[
                        ward_polygons.intersects(area_polygon)
                    ] if pandas.notna(area_polygon) else pandas.Series([])

                    def _location_generator():
                        for llm_location_suggestion_list in llm_locations:
                            for llm_location in llm_location_suggestion_list:
                                # first, try with the GPT location list
                                location_shape = _geocode_location(llm_location, area_polygon)

                                if location_shape is not None:
                                    yield location_shape
                                    break

                                # next, try with the intersecting wards
                                for ward in intersecting_wards.index:
                                    location_shape = _geocode_location(llm_location.split(',')[0] + f", Ward {ward}",
                                                                       area_polygon)
                                    if location_shape is not None:
                                        yield location_shape

                    # Assemble list of location suggestions
                    location_polygons = set(_location_generator())

                    # combining results
                    if len(location_polygons) > 0:
                        logging.debug(f"Merging {len(location_polygons)} polygons for {record_index}")
                        # merging all the polygons together
                        record_polygon = shapely.unary_union(list(location_polygons))

                    # otherwise, also falling back to area polygon
                    else:
                        logging.warning(f"Location geocoding failed for {record_index}, "
                                        f"falling back to {record['area_type']} - {record['area']} polygon")
                        record_polygon = area_polygon

                # rounding the precision
                self.data.loc[record_index, "geospatial_footprint"] = shapely.wkt.dumps(record_polygon,
                                                                                        rounding_precision=6)


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

    logging.info("Add[ing] geospatial footprints")
    sa_augmenter.lookup_location_geospatial_footprint()
    logging.info("Add[ed] geospatial footprints")

    logging.info("Inferr[ing] Suburbs...")
    sa_augmenter.infer_area("Official planning suburbs", "OFC_SBRB_NAME", "inferred_suburbs")
    logging.info("...Inferr[ed] Suburbs")

    logging.info("Inferr[ing] Wards...")
    sa_augmenter.infer_area("Wards", "WARD_NAME", "inferred_wards", "WARD_YEAR == 2021")
    logging.info("...Inferr[ed] Wards")

    logging.info("Look[ing] up Image Filenames...")
    sa_augmenter.lookup_geospatial_image_link()
    logging.info("...Look[ed] up Image Filenames")

    logging.info("Wr[iting] to Minio...")
    sa_augmenter.write_data_to_minio(sa_augmenter.data)
    logging.info("...Wr[ote] to Minio")
