from __future__ import annotations

from typing import Sequence, Optional

import numpy as np

from data_integration_pipeline.common.core.base_singletons import BaseSingleton
from data_integration_pipeline.settings import EMBEDDING_DIMENSIONS, EMBEDDING_MODEL_NAME
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.common.core.models.templates.base_model_embedding import BaseModelEmbedding
from data_integration_pipeline.common.core.utils import get_timestamp


class EmbeddingClient(BaseSingleton):
    """Thin wrapper around ``jina-embeddings-v5-text-nano`` via ``sentence-transformers``.

    Singleton — the model is loaded once and reused across calls.

    Jina v5 uses task-specific LoRA adapters selected via the ``task`` parameter
    and asymmetric prefixes for retrieval (``prompt_name="query"`` vs
    ``prompt_name="document"``).
    """

    @staticmethod
    def get_device() -> str:
        import torch

        return 'cuda' if torch.cuda.is_available() else 'cpu'

    def __init__(self, model_name: str | None = None, truncate_dim: int | None = None) -> None:
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name or EMBEDDING_MODEL_NAME
        self.truncate_dim = truncate_dim or EMBEDDING_DIMENSIONS
        logger.info(f'Loading model {self.model_name}')
        device = self.get_device()
        self._model: SentenceTransformer = SentenceTransformer(self.model_name, trust_remote_code=True, device=device)
        logger.info(f'Model {self.model_name} loaded on device {device}')

    def _encode(self, texts: list[str], prompt_name: Optional[str] = None, batch_size: int = 64) -> np.ndarray:
        kwargs: dict = dict(
            inputs=texts,
            task='retrieval',
            batch_size=batch_size,
            show_progress_bar=False,
            truncate_dim=self.truncate_dim,
        )
        if prompt_name is not None:
            kwargs['prompt_name'] = prompt_name
        return self._model.encode(**kwargs)

    # ------------------------------------------------------------------
    # Document-side encoding (index time)
    # ------------------------------------------------------------------

    def __embed_document(self, text: str) -> np.ndarray:
        """Embed a single document for indexing. Returns a 1-D numpy array."""
        return self._encode([text], prompt_name='document')[0]

    def __embed_documents(self, texts: Sequence[str], batch_size: int = 64) -> np.ndarray:
        """Embed multiple documents. Returns a 2-D array (n_texts, dimensions)."""
        return self._encode(list(texts), prompt_name='document', batch_size=batch_size)

    def embed_document(self, text: str) -> list[float]:
        """Embed a document and return a plain Python list for ES ``dense_vector`` fields."""
        return BaseModelEmbedding(
            embedding=self.__embed_document(text).tolist(),
            dimensions=self.truncate_dim,
            timestamp=get_timestamp(),
            model_id=self.model_name,
        )

    def embed_documents(self, texts: Sequence[str], batch_size: int = 64) -> list[list[float]]:
        """Embed a batch of documents and return a list of plain Python lists."""
        return [
            BaseModelEmbedding(
                embedding=emb.tolist(),
                dimensions=self.truncate_dim,
                timestamp=get_timestamp(),
                model_id=self.model_name,
            )
            for emb in self.__embed_documents(texts, batch_size=batch_size)
        ]

    # ------------------------------------------------------------------
    # Query-side encoding (search time)
    # ------------------------------------------------------------------

    def __embed_query(self, text: str) -> np.ndarray:
        """Embed a query for retrieval. Returns a 1-D numpy array."""
        return self._encode([text], prompt_name='query')[0]

    def embed_query(self, text: str) -> list[float]:
        """Embed a query and return a plain Python list for ES KNN search."""
        return self.__embed_query(text).tolist()

    # ------------------------------------------------------------------
    # Record-level helper (index time)
    # ------------------------------------------------------------------

    def embed_record(self, dict_to_embed: dict) -> dict[str, list[float]]:
        """Produce ``embedding_<field>`` vectors for every non-null embeddable field.

        Accepts a dict with keys matching :data:`EMBEDDING_FIELDS` and returns
        a dict keyed ``embedding_<field>`` → list[float] for each field that
        had a non-empty value.

        Array fields (``product_services``, ``market_niches``, ``business_model``)
        are joined with ``' | '`` before encoding.
        """
        texts: list[str] = []
        field_names: list[str] = []
        for field, value in dict_to_embed.items():
            if not value:
                continue
            field_names.append(field)
            text = ' | '.join(value) if isinstance(value, list) else str(value)
            texts.append(text)

        if not texts:
            return {}

        embeddings = self._encode(texts, prompt_name='document')
        return {f'embedding_{name}': emb.tolist() for name, emb in zip(field_names, embeddings, strict=True)}


if __name__ == '__main__':
    test_doc = {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description': 'The company provides software for fraud detection, risk analytics, and digital banking workflows for financial institutions.',
        'product_services': 'AI underwriting\nFraud detection platform\nRisk monitoring\nBanking analytics dashboards',
        'market_niches': 'Digital Banking\nPayments\nFinancial Infrastructure\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    }
    embedding_client = EmbeddingClient()
    # print(embedding_client.embed_record(test_doc))
    print(len(embedding_client.embed_document('This is a test description')))
    # print(embedding_client.embed_documents(['This is a test description', 'This is a test description']))
    # print(embedding_client.embed_document_to_list('This is a test description'))
    # print(embedding_client.embed_documents_to_list(['This is a test description', 'This is a test description']))
    # print(embedding_client.embed_query('This is a test query'))
    # print(embedding_client.embed_query_to_list('This is a test query'))
