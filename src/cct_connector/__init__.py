import pathlib

SERVICE_ALERTS_PREFIX = "service-alerts"

# Service Alerts datasets
RAW_SA_NAME = f"{SERVICE_ALERTS_PREFIX}.service-alerts"
FIXED_SA_NAME = f"{SERVICE_ALERTS_PREFIX}.sanitised-service-alerts"
AUGMENTED_SA_NAME = f"{SERVICE_ALERTS_PREFIX}.augmented-service-alerts"
SA_EMAIL_NAME = f"{SERVICE_ALERTS_PREFIX}.service-alerts-emails"
SA_WA_NAME = f"{SERVICE_ALERTS_PREFIX}.service-alerts-whatsapps"
PHONE_NUMBER_LOOKUP_NAME = f"{SERVICE_ALERTS_PREFIX}.phone-number-lookup"

# Other datasets
FIXED_SN_MINIO_NAME = "sap-r3-connector.sanitised-service-notifications"

# Other misc data constants
LATEST_PREFIX = "current/"
AUGMENTER_SALT = "service-alert-augmenter-2024-03-21T02:30"
PHONE_NUMBER_LOOKUP_FILE = "phone-number-lookup.json"

# Dataset columns
CHECKSUM_COLUMN = "InputChecksum"
TWEET_COL = "tweet_text"
TOOT_COL = "toot_text"
ID_COL = "Id"
FOOTPRINT_COL = "footprint_id"
GEOSPATIAL_COL = "geospatial_footprint"
SUMMARY_COL = "summary"

# Output Data
SERVICE_ALERTS_S3_BUCKET = "coct-service-alerts"
IMAGE_LINK_TEMPLATE = "https://lake.capetown.gov.za/service-alerts.maps/{image_filename}.png"

TRINO_DATASET = '"internal"."service_alerts"."augmented_service_alerts"'
