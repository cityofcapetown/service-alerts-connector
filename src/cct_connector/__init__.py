import pathlib

SERVICE_ALERTS_PREFIX = "service-alerts"

# Service Alerts datasets
RAW_SA_NAME = f"{SERVICE_ALERTS_PREFIX}.service-alerts"
FIXED_SA_NAME = f"{SERVICE_ALERTS_PREFIX}.sanitised-service-alerts"
AUGMENTED_SA_NAME = f"{SERVICE_ALERTS_PREFIX}.augmented-service-alerts"
SA_EMAIL_NAME = f"{SERVICE_ALERTS_PREFIX}.service-alerts-emails"

# Other datasets
FIXED_SN_MINIO_NAME = "sap-r3-connector.sanitised-service-notifications"

# Other misc data constants
LATEST_PREFIX = "current/"
AUGMENTER_SALT = "service-alert-augmenter-2024-03-21T02:30"

# Dataset columns
CHECKSUM_COLUMN = "InputChecksum"
TWEET_COL = "tweet_text"
TOOT_COL = "toot_text"
ID_COL = "Id"
IMAGE_COL = "image_filename"

# Output Data
SERVICE_ALERTS_S3_BUCKET = "coct-service-alerts"

