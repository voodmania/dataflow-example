import tempfile
from pathlib import Path, PurePath

import pandas as pd
import apache_beam as beam
from google.api_core.exceptions import ClientError
from google.cloud import bigquery, storage

from utils.logger import logging

logger = logging.getLogger(__name__)


class ProcessFile(beam.DoFn):
    def __init__(self, beam_args):
        self.beam_args = beam_args
        self.bq_path = beam_args.get('bq_path')

    def process(self, gcs_url):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logger.info('ProcessFile(%s) init', gcs_url)
            filepath = PurePath(gcs_url)
            status, status_details = 'DONE', ''

            tmp_filepath = Path(tmpdirname) / filepath.parts[3]
            tmp_filepath.mkdir(parents=True)
            tmp_filepath = tmp_filepath / filepath.name

            try:
                bucket = storage.Client().get_bucket(filepath.parts[1])
                blob = bucket.blob('/'.join(filepath.parts[2:]))
                blob.download_to_filename(tmp_filepath)
            except ClientError:
                status, status_details = 'ERROR', 'File not found'
                logger.warning('ProcessFile.process(%s) file not found', gcs_url)

            if status == 'DONE':
                # Process file
                try:
                    df_output = pd.read_csv(tmp_filepath)

                    table_schema = [
                        bigquery.SchemaField(f'test_integer', bigquery.enums.SqlTypeNames.INTEGER),
                        bigquery.SchemaField(f'test_string', bigquery.enums.SqlTypeNames.STRING),
                    ]

                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        schema=table_schema,
                        autodetect=False,
                        source_format=bigquery.SourceFormat.PARQUET,
                    )

                    job = bigquery.Client().load_table_from_dataframe(
                        df_output,
                        f'{self.bq_path}.table',
                        job_config=job_config
                    )
                    job.result()

                    result = True
                except Exception as exc:
                   status, status_details = 'ERROR', str(exc)
                   logger.exception('ProcessTelusRaster.process(%s) exception: %s', gcs_url, exc)
                   tb = exc.__traceback__
                   while tb and tb.tb_frame.f_code.co_name != 'bq_to_arrow_array':
                        tb = tb.tb_next
                   if tb:
                        logger.info(
                            'ProcessFile.process(%s) series: %s',
                            gcs_url, tb.tb_frame.f_locals.get('series')
                        )

            yield result, gcs_url

