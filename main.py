#!/usr/bin/env python

from pathlib import PurePath

from google.cloud import storage

from utils.beamutils import ProcessFile
from utils.logger import logging


logger = logging.getLogger(__name__)


def run(pipeline_args, known_args):
    """Invoked by the Beam runner"""

    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions

    pipeline_options = PipelineOptions(
        [
            '--no_use_public_ips',
            '--disk_size_gb=24',
            '--number_of_worker_harness_threads=1',
            *pipeline_args,
        ],
        temp_location=f'gs://{known_args.dataflow_bucket}/TEMP/',
        staging_location=f'gs://{known_args.dataflow_bucket}/STAGING/',
    )

    parse_pipeline_args = dict([z[2:].split('=') for z in pipeline_args if '=' in z])

    bq_path = known_args.bq_path
    beam_args = {
        'bq_path': bq_path,
        'job_name': parse_pipeline_args.get('job_name'),
    }

    _, bucket_name, *rest = PurePath(known_args.gcs_url).parts  # url decomposing
    blobs = storage.Client().list_blobs(bucket_name, prefix='/'.join(rest))

    gcs_urls = [f'gs://{bucket_name}/{_.name}' for _ in blobs]
    logging.info('Files to process: %s', len(gcs_urls))

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Create Beam' >> beam.Create(gcs_urls)
            | 'Process Raster' >> beam.ParDo(ProcessFile(beam_args))
        )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs_url', type=str)
    parser.add_argument('--bq_path', type=str)
    parser.add_argument('--dataflow_bucket', type=str)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)

