from __future__ import annotations

from typing import Optional, Any

from pydantic import (
    model_validator,
    Field,
)
from datetime import datetime
from data_integration_pipeline.common.core.models.templates.base_models import BaseInnerModel
from data_integration_pipeline.settings import EMBEDDING_DIMENSIONS


class BaseModelEmbedding(BaseInnerModel):
    embedding: Optional[list[float]] = Field(default=None, description='Embedding vector')
    dimensions: Optional[int] = Field(default=EMBEDDING_DIMENSIONS, description='Dimensions of the embedding')
    timestamp: Optional[datetime] = Field(default=None, description='Timestamp of the embedding')
    model_id: Optional[str] = Field(default=None, description='ID of the model that generated the embedding')

    @model_validator(mode='before')
    @classmethod
    def distribute_flat_data(cls, data: Any) -> Any:
        """
        Takes the flat input dictionary and assigns the exact same dictionary
        to every field. The sub-models will filter what they need
        because they have extra='ignore'.
        """
        if isinstance(data, list):
            return {'embedding': data, 'dimensions': EMBEDDING_DIMENSIONS}
        return data

    @model_validator(mode='after')
    def assert_embedding_dimensions(self) -> 'BaseModelEmbedding':
        if self.embedding is None:
            self.dimensions = None
            return self
        if self.dimensions is None:
            raise ValueError('Dimensions are required')
        assert len(self.embedding) == self.dimensions, f'Embedding dimensions do not match. Expected {self.dimensions}, got {len(self.embedding)}'
        return self


if __name__ == '__main__':
    doc = {
        'embedding': [0.1, 0.2, 0.3],
        'dimensions': 3,
    }
    record = BaseModelEmbedding(**doc)
    print(record)
    # print(BaseModelEmbedding(embedding=[0.1]).model_dump())
