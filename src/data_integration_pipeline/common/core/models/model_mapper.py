import re
from typing import Type, Optional, Literal, Union
from pathlib import Path
from data_integration_pipeline.common.core.models import (
    BaseRecordType,
    Dataset1MetaModel,
    Dataset2MetaModel,
    Dataset3MetaModel,
    IntegratedMetaModel,
)
from data_integration_pipeline.common.core.base_singletons import BaseSingleton

from data_integration_pipeline.common.core.models.templates.base_models import BaseBronzeSchemaRecord, BaseSilverSchemaRecord, BaseGoldSchemaRecord


class ModelMapper(BaseSingleton):
    """
    Maps an input string to a Pydantic model class.
    The input string can be a file path, a database table name, a database column name, etc.
    """

    MAPPINGS = {
        re.compile(r'dataset_1', re.IGNORECASE): Dataset1MetaModel,
        re.compile(r'dataset_2', re.IGNORECASE): Dataset2MetaModel,
        re.compile(r'dataset_3', re.IGNORECASE): Dataset3MetaModel,
        re.compile(r'integrated', re.IGNORECASE): IntegratedMetaModel,
    }

    @staticmethod
    def get_schema_by_stage(
        data_model: Type[BaseRecordType], data_stage: Literal['bronze', 'silver', 'gold']
    ) -> Union[BaseBronzeSchemaRecord, BaseSilverSchemaRecord, BaseGoldSchemaRecord]:
        """
        Matches a filename against MAPPINGS and returns the corresponding
        data stage.
        """
        if data_stage == 'bronze':
            return data_model._bronze_schema
        elif data_stage == 'silver':
            return data_model._silver_schema
        elif data_stage == 'gold':
            return data_model._gold_schema

    # TODO this needs to be improved, we can just get the data stage from the file path
    @staticmethod
    def get_primary_key(input_str: str, data_stage: Optional[Literal['bronze', 'silver', 'gold']] = None) -> str:
        """
        Matches a filename against MAPPINGS and returns the corresponding
        Pydantic model class.
        """
        if not data_stage:
            path_inst = Path(input_str)
            data_stage = path_inst.parts[0]
        for pattern, model_class in ModelMapper.MAPPINGS.items():
            if pattern.search(input_str):
                return ModelMapper.get_schema_by_stage(data_model=model_class, data_stage=data_stage)._primary_key
        # no match
        return None

    @staticmethod
    def get_partition_key(input_str: str, data_stage: Optional[Literal['bronze', 'silver', 'gold']] = None) -> Optional[str]:
        """
        Matches a filename against MAPPINGS and returns the corresponding
        Pydantic model class.
        """
        if not data_stage:
            path_inst = Path(input_str)
            data_stage = path_inst.parts[0]
        for pattern, model_class in ModelMapper.MAPPINGS.items():
            if pattern.search(input_str):
                return ModelMapper.get_schema_by_stage(data_model=model_class, data_stage=data_stage)._partition_key
        # no match
        return None

    @staticmethod
    def get_data_model(input_str: str) -> Type[BaseRecordType] | None:
        """
        Matches a filename against MAPPINGS and returns the corresponding
        Pydantic model class.
        """
        for pattern, model_class in ModelMapper.MAPPINGS.items():
            if pattern.search(input_str):
                return model_class
        # no match
        return None


if __name__ == '__main__':
    data_model = ModelMapper.get_data_model('business_entity_registry.csv')
    print(data_model)
    data_model = ModelMapper.get_data_model(
        '/home/pedroq/personal/data_integration_pipeline/tmp/bronze/sub_contractors_registry/sub_contractors_registry.csv'
    )
    print(data_model)
