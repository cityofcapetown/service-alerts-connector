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
AUGMENTER_SALT = "service-alert-augmenter-2024-03-09T23:38"

# Dataset columns
CHECKSUM_COLUMN = "InputChecksum"
TWEET_COL = "tweet_text"
TOOT_COL = "toot_text"
ID_COL = "Id"

# Output Data
SERVICE_ALERTS_S3_BUCKET = "coct-service-alerts"

