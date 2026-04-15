from datetime import datetime
from functools import cached_property
from typing import Any, ClassVar, Optional, TypeVar, Union

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field

from data_integration_pipeline.common.core.models.schema_converter import PyarrowSchemaGenerator
from data_integration_pipeline.common.core.utils import get_timestamp

BASE_CONFIG_DICT = ConfigDict(
    populate_by_name=True,
    extra='ignore',
    str_strip_whitespace=True,
    arbitrary_types_allowed=True,
    revalidate_instances='never',
)


class BaseBronzeSchemaRecord(BaseModel):
    model_config = BASE_CONFIG_DICT


class BaseElasticsearchSchemaRecord(BaseModel):
    model_config = BASE_CONFIG_DICT


class BaseSilverSchemaRecord(BaseModel):
    model_config = BASE_CONFIG_DICT
    _primary_key: ClassVar[str]
    _partition_key: ClassVar[str]

    load_ldts: Optional[datetime] = Field(default=None, repr=False)


class BaseGoldSchemaRecord(BaseModel):
    model_config = BASE_CONFIG_DICT
    _primary_key: ClassVar[str]
    _partition_key: ClassVar[Optional[str]] = None
    # comes from the delta table
    load_ldts: datetime = Field(default=None, repr=False)
    # time of the sync operation
    sync_ldts: datetime = Field(default_factory=get_timestamp, repr=False)


class BaseInnerModel(BaseModel):
    model_config = BASE_CONFIG_DICT


class BaseMetaModel(BaseModel):
    model_config = ConfigDict(strict=True, populate_by_name=True, from_attributes=True)
    data_source: ClassVar[str]
    raw_record: Optional[dict] = Field(default=None, exclude=True, repr=False)
    _bronze_schema: ClassVar[Optional[type[BaseBronzeSchemaRecord]]]
    _silver_schema: ClassVar[Optional[type[BaseSilverSchemaRecord]]]
    _pa_silver_schema: ClassVar[pa.Schema]
    _gold_schema: ClassVar[Optional[type[BaseGoldSchemaRecord]]] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not cls._bronze_schema and not cls._silver_schema and not cls._gold_schema:
            raise ValueError(f'{cls.__name__} must have at least one schema configured')
        cls._pa_silver_schema = PyarrowSchemaGenerator(cls._silver_schema, allow_losing_tz=True).run()

    def __str__(self) -> str:
        res = [f'{self.__class__.__name__}(source={self.data_source}']
        if self._bronze_schema:
            res.append(f'bronze={self.bronze_record}')
        if self._silver_schema:
            res.append(f'silver={self.silver_record}')
        if self._gold_schema:
            res.append(f'gold={self.gold_record}')
        res.append(f'pk={getattr(self, "primary_key", None)}, partition={getattr(self, "partition_key", None)})')
        return '\n'.join(res)

    @cached_property
    def bronze_record(self) -> Optional['BaseBronzeSchemaRecord']:
        if not self._bronze_schema:
            return None
        if not self.raw_record:
            return None
        return self._bronze_schema(**self.raw_record)

    @cached_property
    def silver_record(self) -> Optional['BaseSilverSchemaRecord']:
        if not self._silver_schema:
            return None
        if not self.bronze_record:
            return None
        return self._silver_schema.model_validate(self.bronze_record, from_attributes=True)

    @cached_property
    def gold_record(self) -> Optional['BaseGoldSchemaRecord']:
        if not self._gold_schema:
            return None
        if not self.silver_record:
            return None
        return self._gold_schema.model_validate(self.silver_record, from_attributes=True)

    @classmethod
    def from_raw_record(cls, raw_record: dict, **kwargs: Any) -> 'BaseMetaModel':
        instance = cls(**kwargs)
        instance.raw_record = raw_record
        instance.bronze_record = instance._bronze_schema(**raw_record)
        return instance

    @classmethod
    def from_bronze_record(cls, bronze_record: Union[dict, BaseBronzeSchemaRecord], **kwargs: Any) -> 'BaseMetaModel':
        instance = cls(**kwargs)
        if isinstance(bronze_record, dict):
            instance.bronze_record = instance._bronze_schema(**bronze_record)
        else:
            instance.bronze_record = bronze_record
        return instance

    @classmethod
    def from_silver_record(cls, silver_record: Union[dict, BaseSilverSchemaRecord], **kwargs: Any) -> 'BaseMetaModel':
        instance = cls(**kwargs)
        if isinstance(silver_record, dict):
            instance.silver_record = instance._silver_schema(**silver_record)
        else:
            instance.silver_record = silver_record
        return instance

    @classmethod
    def from_gold_record(cls, gold_record: Union[dict, BaseGoldSchemaRecord], **kwargs: Any) -> 'BaseMetaModel':
        if not cls._gold_schema:
            raise ValueError(f'{cls.__name__} has no gold schema configured')
        instance = cls(**kwargs)
        if isinstance(gold_record, dict):
            instance.gold_record = instance._gold_schema(**gold_record)
        else:
            instance.gold_record = gold_record
        return instance


BaseRecordType = TypeVar('BaseRecord', bound=BaseBronzeSchemaRecord)
