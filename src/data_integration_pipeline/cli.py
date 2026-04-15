from __future__ import annotations

from typing import Sequence, Optional

import click

from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.settings import ARCHIVED_BRONZE_DATA_FOLDER, CODE_VERSION, SERVICE_NAME


@click.group(help=f'{SERVICE_NAME} CLI', context_settings={'help_option_names': ['-h', '--help']})
@click.version_option(version=CODE_VERSION, prog_name=SERVICE_NAME)
def cli() -> None:
    pass


@cli.command('download-bronze', help='Downloads test data from Hetzner public bucket, data is stored in tests/data')
def download_bronze() -> None:
    from data_integration_pipeline.bronze.jobs.download_bronze import DownloadBronzeJob

    DownloadBronzeJob().run()


@cli.command('upload-bronze', help='Upload test data from tests/data/ to S3 bronze/')
@click.option('-ds', '--data-source', default=None, type=str, show_default=True, help='Data source to upload.')
def upload_bronze(data_source: Optional[str] = None) -> None:
    from data_integration_pipeline.bronze.jobs.upload_bronze import UploadBronzeJob

    UploadBronzeJob(data_source=data_source).run()


@cli.command('chunk-bronze', help='Chunks bronze files to s3/chunked_bronze and archives bronze data to s3/archived_bronze')
@click.option('-ds', '--data-source', default=None, type=str, show_default=True, help='Data source to chunk.')
def chunk_bronze(data_source: Optional[str] = None) -> None:
    from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob

    ChunkBronzeJob(data_source=data_source).run()


@cli.command('process-bronze', help='Validate bronze chunks and write processed parquet files.')
@click.option('-ds', '--data-source', default=None, type=str, show_default=True, help='Data source to process.')
def process_bronze(data_source: Optional[str] = None) -> None:
    from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob

    ProcessBronzeJob(data_source=data_source).run()


@cli.command('load-bronze', help='Write processed parquet chunks to silver Delta tables.')
@click.option('-ds', '--data-source', default=None, type=str, show_default=True, help='Data source to load.')
def load_bronze(data_source: Optional[str] = None) -> None:
    from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob

    LoadBronzeJob(data_source=data_source).run()


@cli.command(
    'list-bronze-errors',
    help=f'Emit JSON report of all validation error parquet files under {ARCHIVED_BRONZE_DATA_FOLDER} (metadata + listing).',
)
def list_bronze_errors() -> None:
    from data_integration_pipeline.bronze.jobs.list_bronze_errors import ListBronzeErrorsJob

    ListBronzeErrorsJob().run()


@cli.command('audit-silver', help='Run Great Expectations audits on silver Delta tables.')
def audit_silver() -> None:
    from data_integration_pipeline.silver.jobs.audit_silver import AuditSilverDataJob

    AuditSilverDataJob().run()


@cli.command('integrate-silver', help='Join per-source silver Delta tables into integrated silver Delta table using Spark.')
def integrate_silver() -> None:
    from data_integration_pipeline.silver.jobs.integrate_silver import IntegrateSilverJob

    IntegrateSilverJob().run()


@cli.command('sync-pg', help='Syncs integrated records to Postgres.')
def sync_postgres() -> None:
    from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob

    SyncPostgresJob().run()


@cli.command('sync-es', help='Syncs organizations and description_and_keywords to Elasticsearch.')
def sync_es() -> None:
    from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob

    SyncOrganizationsElasticsearchJob().run()


@cli.command('optimize-delta', help='Optimize all Delta tables using environment settings.')
def optimize_delta() -> None:
    from data_integration_pipeline.silver.jobs.optimize_delta_tables import OptimizeDeltaTablesJob

    OptimizeDeltaTablesJob().run()


@cli.command('vacuum-delta', help='Vacuum all Delta tables using environment settings.')
def vacuum_delta() -> None:
    from data_integration_pipeline.silver.jobs.vacuum_delta_tables import VacuumDeltaTablesJob

    VacuumDeltaTablesJob().run()


@cli.command('pg-report', help='Create or replace the integrated_records_report summary view in Postgres.')
def pg_report() -> None:
    from data_integration_pipeline.gold.jobs.create_pg_report import CreatePostgresReportJob

    CreatePostgresReportJob().run()


@cli.command('audit-docs', help='Serve the Great Expectations audit report locally.')
@click.option('-p', '--port', default=8088, type=int, show_default=True, help='Port for the HTTP server.')
def audit_docs(port: int) -> None:
    import http.server
    import os

    docs_dir = os.path.join('tmp', 'audits', 'gx', 'uncommitted', 'data_docs', 'local_site')
    if not os.path.isdir(docs_dir):
        logger.error(f'Data Docs directory not found at {docs_dir}. Run audit-silver first.')
        return
    logger.info(f'Serving audit docs at http://localhost:{port} — Ctrl+C to stop.')
    os.chdir(docs_dir)
    try:
        http.server.HTTPServer(('', port), http.server.SimpleHTTPRequestHandler).serve_forever()
    except KeyboardInterrupt:
        logger.info('Stopping audit docs server...')


@cli.command('streamlit', help='Launch the Streamlit exploration UI.')
@click.option('-p', '--port', default=8501, type=int, show_default=True, help='Port for Streamlit.')
def streamlit(port: int) -> None:
    import subprocess

    logger.info(f'Serving streamlit at http://localhost:{port} — Ctrl+C to stop.')
    cmd = [
        'streamlit',
        'run',
        'src/data_integration_pipeline/streamlit/app.py',
        '--server.port',
        str(port),
        '--server.fileWatcherType=none',
        '--server.headless=true',
    ]

    raise SystemExit(subprocess.call(cmd))


@cli.command('load-output-data', help='[DEV] Load output data from the S3 bucket to the tmp folder and load into Minio, PG, and ES.')
def load_output_data() -> None:
    from data_integration_pipeline.gold.jobs.load_output_data import LoadOutputDataJob

    LoadOutputDataJob().run()


@cli.command('pipeline', help='Run the full pipeline sequentially.')
def pipeline() -> None:
    from data_integration_pipeline.bronze.jobs.download_bronze import DownloadBronzeJob
    from data_integration_pipeline.bronze.jobs.upload_bronze import UploadBronzeJob
    from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
    from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob
    from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob
    from data_integration_pipeline.silver.jobs.integrate_silver import IntegrateSilverJob
    from data_integration_pipeline.silver.jobs.optimize_delta_tables import OptimizeDeltaTablesJob
    from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob
    from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob
    from data_integration_pipeline.gold.jobs.create_pg_report import CreatePostgresReportJob

    DownloadBronzeJob().run()
    UploadBronzeJob().run()
    ChunkBronzeJob().run()
    ProcessBronzeJob().run()
    LoadBronzeJob().run()
    IntegrateSilverJob().run()
    OptimizeDeltaTablesJob().run()
    SyncPostgresJob().run()
    SyncOrganizationsElasticsearchJob().run()
    CreatePostgresReportJob().run()


def main(argv: Sequence[str] | None = None) -> int:
    try:
        cli.main(args=list(argv) if argv is not None else None, prog_name='dip')
        return 0
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        return code
