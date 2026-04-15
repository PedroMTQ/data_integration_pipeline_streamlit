import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import uuid6


def get_run_id() -> str:
    return str(uuid6.uuid7())


def get_timestamp(as_str=False) -> str:
    if as_str:
        return datetime.now(timezone.utc).isoformat()
    else:
        return datetime.now(timezone.utc)


def std(v: Any) -> str:
    """
    Standardizes values for hashing: Trim, Upper, and Null handling.
    """
    if v is None:
        return '^~'
    clean_v = str(v).strip()
    if clean_v == '':
        return '^~'
    return clean_v.upper()


def get_hash(str_to_encode: Optional[str] = None) -> str:
    """
    Standardizes and hashes a single business key.
    """
    if str_to_encode is None:
        # Data Vault standard: Use a placeholder for NULLs
        # (e.g., -1 or a specific MD5 of an empty string)
        str_to_encode = '~~NULL~~'
    # 2. Hash (MD5 is common for performance; SHA-256 for security)
    return hashlib.sha256(std(str_to_encode).encode('utf-8')).hexdigest()


def get_hash_for_payload(payload: dict[str, Any]) -> str:
    """
    Generates a SHA-256 Hash Diff for change detection in Satellites or for other dicts (e.g., links pks).
    Excludes the business key (anchor) and existing metadata.
    """
    return get_hash(prepare_payload_for_hashing(payload))


def prepare_payload_for_hashing(payload: dict[str, Any]) -> str:
    # Use your sorted logic to ensure determinism
    # We exclude the anchor key because changes to IDs are new entities, not updates.
    std_payload = [std(v) for k, v in sorted(payload.items())]
    return '||'.join(std_payload)


def get_uuid5(input_id: str) -> str:
    """Generate a deterministic UUID from unique_id using uuid5 with NAMESPACE_OID."""
    if not (input_id and input_id.strip()):
        return '00000000-0000-0000-0000-000000000000'
    return str(uuid.uuid5(uuid.NAMESPACE_OID, input_id.strip()))


def get_hk(source_id: str, source_name: str) -> str:
    """
    Generates a Data Vault 2.0 compliant SHA-256 Hash Key (HK).
    This is the 'Anchor' for your Hub.
    """
    if not source_id or not source_name:
        raise Exception('Missing source_id or source_name')
    s_id = std(source_id)
    s_name = std(source_name)
    # Standard DV2.0: Separator prevents '1'+'23' colliding with '12'+'3'
    anchor_key_id = f'{s_id}||{s_name}'
    return get_uuid5(anchor_key_id)


if __name__ == '__main__':
    print(get_uuid5('1234567890'))
    print(get_hk('1234567890', 'test'))
    print(get_hk('1234567890', 'test2'))
