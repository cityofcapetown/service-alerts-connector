import hashlib
import multiprocessing
import logging

from db_utils import minio_utils
import pandas

from cct_connector import CHECKSUM_COLUMN, LATEST_PREFIX, ID_COL


N_PROCS = max(1, min(8, int(multiprocessing.cpu_count() / 2)))  # cap processor use at 8
N_CHUNKS = N_PROCS * 4
MIN_CHUNKSIZE = 10000


def _calculate_checksums(args):
    data_df, stage_salt = args

    checksums = data_df.apply(
        lambda row: hashlib.md5(
            str.encode("".join(map(str, row.values)) + stage_salt)
        ).hexdigest(),
        axis='columns'
    )

    return checksums


def _calculate_checksums_parallel(data_df, stage_salt=""):
    with multiprocessing.Pool(N_PROCS) as pool:
        chunk_size = max(data_df.shape[0] // N_CHUNKS + 1, MIN_CHUNKSIZE)

        # Chunk up the dataframe
        df_chunks = (
            (data_df.iloc[i * chunk_size:(i + 1) * chunk_size], stage_salt)
            for i in range(N_CHUNKS)
        )

        # Chunking work across processors
        hash_chunks = pool.imap(
            _calculate_checksums,
            df_chunks
        )

        # Pull it all back together
        checksums = pandas.concat(hash_chunks)

    return checksums


def _get_checksum_indices(cache_data_df, data_checksums):
    cache_index = pandas.MultiIndex.from_arrays([
        cache_data_df.index.values,
        cache_data_df[CHECKSUM_COLUMN].values
    ])

    data_index = pandas.MultiIndex.from_arrays([
        data_checksums.index.values,
        data_checksums.values
    ])

    return cache_index, data_index


class ServiceAlertsBase:

    def __init__(self, minio_read_access, minio_read_secret, minio_read_classification, minio_read_name,
                 minio_write_access=None, minio_write_secret=None, minio_write_classification=None,
                 minio_write_name=None, stage_cache_salt="", use_cached_values=True,
                 index_col=None):
        self.minio_read_access = minio_read_access
        self.minio_read_secret = minio_read_secret
        self.minio_read_classification = minio_read_classification
        self.minio_read_name = minio_read_name

        # If the write variables are not set, assuming that it is same as the read
        self.minio_write_access = minio_read_access if minio_write_access is None else minio_write_access
        self.minio_write_secret = minio_read_secret if minio_write_secret is None else minio_write_secret
        self.minio_write_classification = minio_read_classification if minio_write_classification is None \
                                                                    else minio_write_classification
        self.minio_write_name = minio_read_name if minio_write_name is None else minio_write_name
        self.stage_cache_salt = stage_cache_salt
        self.use_cached_values = use_cached_values
        self.index_col = index_col

        # Internal class attributes
        self.opportunistic_skip = True
        self.data = None
        self.cache_data = None

    def get_data_from_minio(self, minio_read_name=None, data_size_limit=20):
        """
        Gets the current Service Alert data from Minio.

        :return: Pandas dataframe of data currently in Minio
        """

        # If the read name is set, allow it be overridden
        minio_read_name = self.minio_read_name if minio_read_name is None else minio_read_name

        data = minio_utils.minio_to_dataframe(
            minio_bucket=minio_read_name,
            minio_key=self.minio_read_access,
            minio_secret=self.minio_read_secret,
            data_classification=self.minio_read_classification,
        )
        if self.index_col and self.index_col in data.columns:
            data.set_index(self.index_col, inplace=True)

        logging.debug(f"data.columns={data.columns}")

        if "index" in data.columns:
            logging.warning("Dropping spurious index column")
            data.drop(columns=["index"], inplace=True)

        if self.use_cached_values:
            # While we're waiting for the previous results to download, calculate the checksums
            logging.debug("Calculat[ing] Checksums on existing data")
            checksums = _calculate_checksums_parallel(data, self.stage_cache_salt)
            logging.debug("Calculat[ed] Checksums on existing data")

            logging.debug("Wait[ing] for cached data.")
            self.cache_data = minio_utils.minio_to_dataframe(
                self.minio_write_name,
                self.minio_write_access,
                self.minio_write_secret,
                self.minio_write_classification,
                use_cache=False,
            )
            logging.debug("Wait[ed] for cached data.")
            logging.debug(f"self.cache_data.columns={self.cache_data.columns}")

            if CHECKSUM_COLUMN in self.cache_data.columns:
                logging.debug("Generat[ing] the checksum indices (used for comparison)")
                cache_data_index, data_index = _get_checksum_indices(self.cache_data, checksums)
                logging.debug("Generat[ed] the checksum indices")

                # The data in our cache that *is in* our input data (don't want to keep old values around)
                cache_mask = cache_data_index.isin(data_index)
                # The data in our dataset that *is not in* the cache - this is the data we want to work on
                data_mask = ~(data_index.isin(cache_data_index))

                logging.debug(f"new data size={data_mask.sum()}")
                logging.debug(f"cached data size={cache_mask.sum()}")

                # Splitting data accordingly
                logging.debug(f" (pre-cache filter) {data.shape=}")
                data = data[data_mask].tail(data_size_limit)
                checksums = checksums[data_mask].tail(data_size_limit)
                logging.debug(f"(post-cache filter) {data.shape=}")
                self.cache_data = self.cache_data[cache_mask]

            else:
                logging.warning("Not using caching - no checksums in previous results")
                del self.cache_data
                self.use_cached_values = False

            # Setting checksum column values
            data[CHECKSUM_COLUMN] = checksums

        return data

    def write_data_to_minio(self, data, file_format="parquet"):
        """
        Writes current SAP R3 data to minio.

        :return:
        """
        if self.use_cached_values and self.opportunistic_skip and self.data.shape[0] == 0:
            logging.warning("Skipping write to Minio - nothing has changed")
            return True

        elif self.use_cached_values:
            logging.debug("(pre-cache append) data.shape={}".format(data.shape))
            logging.debug(f"(pre-cache append) data.columns={data.columns}")
            data = pandas.concat([data, self.cache_data])
            logging.debug(f"(post-cache append) data.columns={data.columns}")
            logging.debug("(post-cache append) data.shape={}".format(data.shape))

        df_write_kwargs = {"allow_truncated_timestamps": True, 'coerce_timestamps': 'ms'} if file_format == 'parquet' else {}
        result = minio_utils.dataframe_to_minio(
            data,
            minio_bucket=self.minio_write_name,
            minio_key=self.minio_write_access,
            minio_secret=self.minio_write_secret,
            data_classification=self.minio_write_classification,
            prune=5,
            file_format=file_format,
            use_cache=False,
            latest_copy=True,
            latest_prefix=LATEST_PREFIX,
            **df_write_kwargs
        )

        return result
