from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.bronze.core.metadata import BronzeToSilverProcessingMetadata
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import ARCHIVED_BRONZE_DATA_FOLDER, METADATA_FILE_NAME, METADATA_FILE_PATTERN

_ERRORS_OBJECT_PATTERN = re.compile(r'/errors/[^/]+\.parquet$', re.IGNORECASE)


def _run_metadata_path_for_error_key(error_object_key: str) -> str:
    """…/run_dir/errors/chunk.parquet → …/run_dir/metadata.json"""
    run_dir = Path(error_object_key).parent.parent
    return (run_dir / METADATA_FILE_NAME).as_posix()


class ListBronzeErrorsJob:
    def __init__(self) -> None:
        self._storage = CloudStorageClient()

    def _list_under(self, prefix: str, pattern: re.Pattern) -> list[str]:
        return self._storage.get_files(prefix=prefix, regex_pattern=pattern)

    def _object_exists_cached(self, key: str, cache: dict[str, bool]) -> bool:
        if key not in cache:
            cache[key] = self._storage.file_exists(key)
        return cache[key]

    @staticmethod
    def _error_row(
        path: str,
        *,
        exists: bool,
        in_listing: bool,
        recorded_in_metadata: bool,
    ) -> dict:
        return {
            'path': path,
            'object_exists': exists,
            'listed_in_store': in_listing,
            'recorded_in_metadata': recorded_in_metadata,
        }

    @staticmethod
    def _parent_shell(meta: BronzeToSilverProcessingMetadata) -> dict:
        return {
            'data_source': meta.data_source,
            'run_id': meta.run_id,
            'parent_complete': meta.end_timestamp is not None,
            'input_raw_file': meta.input_raw_file,
            'archived_raw_file': meta.archived_raw_file,
            'parent_end_timestamp': meta.end_timestamp,
            'metrics': meta.metrics.model_dump(),
            'error_paths': [],
        }

    def _load_parent(
        self,
        metadata_path: str,
        read_errors: list[dict],
    ) -> BronzeToSilverProcessingMetadata | None:
        try:
            return BronzeToSilverProcessingMetadata(**self._storage.read_json(metadata_path))
        except Exception as e:
            logger.warning(f'Failed to read parent metadata {metadata_path}: {e}')
            read_errors.append({'parent_metadata_path': metadata_path, 'error': str(e)})
            return None

    def _attach_listing_paths(
        self,
        parents: dict[str, dict],
        listing_paths: set[str],
        exists_cache: dict[str, bool],
    ) -> list[dict]:
        """Attach listing-only paths to known parents; paths with no parent metadata → orphans."""
        orphans: list[dict] = []
        for path in sorted(listing_paths):
            parent_key = _run_metadata_path_for_error_key(path)
            bucket = parents.get(parent_key)
            if bucket is None:
                orphans.append(
                    {
                        'path': path,
                        'object_exists': self._object_exists_cached(path, exists_cache),
                        'inferred_parent_metadata_path': parent_key,
                    }
                )
                continue
            known = {e['path'] for e in bucket['error_paths']}
            if path in known:
                continue
            bucket['error_paths'].append(
                self._error_row(
                    path,
                    exists=self._object_exists_cached(path, exists_cache),
                    in_listing=True,
                    recorded_in_metadata=False,
                )
            )
        return orphans

    @staticmethod
    def _sort_error_paths(parents: dict[str, dict]) -> None:
        for pdata in parents.values():
            pdata['error_paths'].sort(key=lambda e: e['path'])

    @staticmethod
    def _summary(
        parents: dict[str, dict],
        listing_orphans: list[dict],
        metadata_files_scanned: int,
        metadata_read_errors: list[dict],
    ) -> dict:
        rows = [e for pdata in parents.values() for e in pdata['error_paths']]
        distinct_paths = {e['path'] for e in rows} | {o['path'] for o in listing_orphans}

        return {
            'parent_runs_in_report': len(parents),
            'metadata_json_files_scanned': metadata_files_scanned,
            'metadata_json_read_failures': len(metadata_read_errors),
            'distinct_error_object_paths': len(distinct_paths),
            'error_path_rows_from_metadata': sum(1 for e in rows if e['recorded_in_metadata']),
            'error_path_rows_from_listing_only_attached_to_parent': sum(1 for e in rows if not e['recorded_in_metadata']),
            'listing_orphans_no_parent_metadata': len(listing_orphans),
            'paths_recorded_in_metadata_but_missing_in_store': sum(1 for e in rows if e['recorded_in_metadata'] and not e['object_exists']),
        }

    def collect(self) -> dict:
        listing_paths = set(self._list_under(ARCHIVED_BRONZE_DATA_FOLDER, _ERRORS_OBJECT_PATTERN))
        metadata_paths = self._list_under(ARCHIVED_BRONZE_DATA_FOLDER, METADATA_FILE_PATTERN)
        read_errors: list[dict] = []
        parents: dict[str, dict] = {}
        exists_cache: dict[str, bool] = {}

        for meta_path in sorted(metadata_paths):
            meta = self._load_parent(meta_path, read_errors)
            if meta is None:
                continue
            shell = self._parent_shell(meta)
            parents[meta_path] = shell
            for err_path in dict.fromkeys(meta.errors_s3_path or []):
                shell['error_paths'].append(
                    self._error_row(
                        err_path,
                        exists=self._object_exists_cached(err_path, exists_cache),
                        in_listing=err_path in listing_paths,
                        recorded_in_metadata=True,
                    )
                )

        listing_orphans = self._attach_listing_paths(parents, listing_paths, exists_cache)
        self._sort_error_paths(parents)

        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'summary': self._summary(parents, listing_orphans, len(metadata_paths), read_errors),
            'metadata_read_errors': read_errors,
            'parents': parents,
            'listing_orphans': listing_orphans,
        }

    def run(self) -> dict:
        payload = self.collect()
        print(json.dumps(payload, indent=2, default=str))
        return payload

    @staticmethod
    def get_errors_to_process(payload: dict) -> list[dict]:
        errors = []
        for parent in payload['parents'].values():
            for error in parent['error_paths']:
                if error['object_exists']:
                    errors.append(error['path'])
        return errors


if __name__ == '__main__':
    data = ListBronzeErrorsJob().run()
    errors = ListBronzeErrorsJob.get_errors_to_process(data)
    print(errors)
