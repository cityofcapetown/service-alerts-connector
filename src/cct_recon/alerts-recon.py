import copy
import json

import boto3

PREV_SUFFIX = ".prev"
V1_ALERTS_PREFIX = 'alerts/'
V1_1_ALERTS_PREFIX = 'v1.1/service-alert/'
V1_2_ALERTS_PREFIX = 'v1.2/service-alert/'
SNS_ARN = "arn:aws:sns:af-south-1:566800947500:service-alerts"

s3 = boto3.client('s3')
sns = boto3.client('sns')


def lambda_handler(event, context):
    # extracting S3 details from triggering event
    record, *_ = event['Records']
    s3_event = record['s3']
    bucket_name = s3_event['bucket']['name']
    object_name = s3_event['object']['key']
    print(f"{object_name=}")

    # Fetching file from S3
    service_alerts_response = s3.get_object(Bucket=bucket_name, Key=object_name)
    service_alerts_data = json.load(service_alerts_response['Body'])

    # Fetching old data from S3
    old_object_name = object_name + PREV_SUFFIX
    if 'Contents' in s3.list_objects(Bucket=bucket_name, Prefix=old_object_name):
        old_service_alerts_response = s3.get_object(Bucket=bucket_name, Key=old_object_name)
        old_service_alerts_data = json.load(old_service_alerts_response['Body'])
    else:
        old_service_alerts_data = []

    # Creating set of old ID-Status pairs
    old_ids = set([
        f'{old_service_alert["Id"]}-{old_service_alert["status"]}'
        for old_service_alert in old_service_alerts_data
    ])

    # Creating list of new service alerts
    new_service_alerts = [
        service_alert
        for service_alert in service_alerts_data
        if f'{service_alert["Id"]}-{service_alert["status"]}' not in old_ids
    ]
    print(f"{len(old_ids)=}, {len(new_service_alerts)=}")

    for service_alert in new_service_alerts:
        print(f"Writing {service_alert['Id']} to S3")
        # V1 alert
        v1_service_alert = copy.deepcopy(service_alert)
        del v1_service_alert['geospatial_footprint']
        del v1_service_alert['area_type']

        response = s3.put_object(
            Body=json.dumps(v1_service_alert),
            Bucket=bucket_name,
            Key=V1_ALERTS_PREFIX + str(service_alert["Id"]) + ".json",
            ContentType='application/json'
        )

        v1_1_service_alert = copy.deepcopy(service_alert)
        del v1_1_service_alert['status']

        # V1.1 alert
        response = s3.put_object(
            Body=json.dumps(v1_1_service_alert),
            Bucket=bucket_name,
            Key=V1_1_ALERTS_PREFIX + str(service_alert["Id"]),
            ContentType='application/json'
        )

        # V1.2 alert
        response = s3.put_object(
            Body=json.dumps(service_alert),
            Bucket=bucket_name,
            Key=V1_2_ALERTS_PREFIX + str(service_alert["Id"]),
            ContentType='application/json'
        )

    new_service_alert_ids = [
        {"Id": service_alert["Id"]}
        for service_alert in new_service_alerts
    ]

    # Publishing new service alerts to SNS
    if len(new_service_alert_ids):
        print("Publishing to SNS!")
        response = sns.publish(
            TopicArn=SNS_ARN,
            Subject=f"New or updated Service Alerts!",
            Message=json.dumps(new_service_alert_ids)
        )

    # Creating stripped down version of service alerts list for dedupping
    current_service_alerts = [
        {"Id": service_alert["Id"], "status": service_alert["status"]}
        for service_alert in service_alerts_data
    ]
    current_service_alerts_json = json.dumps(current_service_alerts)

    # Writing back to S3 bucket
    s3.put_object(
        Body=current_service_alerts_json,
        Bucket=bucket_name,
        Key=object_name + PREV_SUFFIX,
        ContentType='application/json'
    )

    return {
        'statusCode': 200,
        'body': current_service_alerts_json
    }
