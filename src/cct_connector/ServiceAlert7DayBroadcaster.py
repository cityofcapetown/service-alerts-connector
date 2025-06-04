import logging

from cct_connector import ServiceAlertBroadcaster

WEEK_BOK_CONFIGS = [
    ServiceAlertBroadcaster.ServiceAlertOutputFileConfig(time_window, planned, version, version_cols)
    for time_window in [7]
    for planned in [True, False]
    for version, version_cols in (('v0', ServiceAlertBroadcaster.V0_COLS),
                                  ('v1', ServiceAlertBroadcaster.V1_COLS),
                                  ('v1.1', ServiceAlertBroadcaster.V1_1_COLS),
                                  ('v1.2', ServiceAlertBroadcaster.V1_2_COLS),
                                  ('v1.3', ServiceAlertBroadcaster.V1_3_COLS),)
]

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    logging.info("G[etting] data from Minio...")
    sa_broadcaster = ServiceAlertBroadcaster.ServiceAlertBroadcaster()
    logging.info("...G[ot] data from Minio")

    logging.info("Wr[iting] to S3")
    sa_broadcaster.write_to_s3(WEEK_BOK_CONFIGS)
    logging.info("Wr[ote] to S3")
