"""'
Takes the data from dataset 2 and embeds it, saving it in a dataset3 parquet file, which is used instead of the provided dataset3.json file.
"""

from data_integration_pipeline.common.io.delta_client import DeltaClient
import pyarrow as pa
from data_integration_pipeline.gold.core.embedding_client import EmbeddingClient
import pyarrow.parquet as pq
from data_integration_pipeline.common.core.utils import get_timestamp
from tqdm import tqdm
from data_integration_pipeline.settings import EMBEDDING_DIMENSIONS
from data_integration_pipeline.common.core.models.templates.base_model_embedding import BaseModelEmbedding


def main():
    output_path = 'tests/dataset_3.parquet'
    schema = pa.schema(
        [
            ('url', pa.string()),
            ('global_emb', pa.list_(pa.float32())),
            ('business_description_emb', pa.list_(pa.float32())),
            ('product_services_emb', pa.list_(pa.float32())),
            ('market_niches_emb', pa.list_(pa.float32())),
            ('business_model_emb', pa.list_(pa.float32())),
            ('short_description_emb', pa.list_(pa.float32())),
            ('embedding_updated_at', pa.string()),
        ]
    )

    writer = pq.ParquetWriter(output_path, schema)
    delta_client = DeltaClient()
    table_path = 'silver/integrated/records.delta'
    data_yielder = yield_data(delta_client, table_path)
    total = delta_client.get_count(table_path)
    print(f'Total records to embed: {total}')
    with tqdm(total=total, unit='row', desc='Embedding') as pbar:
        for chunk in data_yielder:
            batch = pa.RecordBatch.from_pylist(chunk, schema=schema)
            writer.write_batch(batch)
            pbar.update(len(chunk))
        writer.close()


# receives a dictionary with a bunch of documetns to embed, where each key is a hash key and the value is the document to embed
def embed_documents(documents: dict, embedding_client: EmbeddingClient) -> dict:
    list_hash_keys = list(documents.keys())
    list_values_to_embed = [documents[k] for k in list_hash_keys]
    embeddings: list[BaseModelEmbedding] = embedding_client.embed_documents(list_values_to_embed)
    return {k: embeddings[i].embedding for i, k in enumerate(list_hash_keys)}


def yield_data(delta_client, table_path, embed_batch_size: int = 512):
    current_time = get_timestamp(as_str=True)
    fields_to_embed = {
        'desc__description_long': 'business_description_emb',
        'desc__description_short': 'short_description_emb',
        'desc__product_services': 'product_services_emb',
        'desc__market_niches': 'market_niches_emb',
        'desc__business_model': 'business_model_emb',
    }
    embedding_client = EmbeddingClient()

    pending_texts: dict[str, str] = {}
    pending_records: list[dict] = []
    pending_mappings: list[list[tuple[str, str]]] = []
    row_counter = 0

    for batch in delta_client.read(table_path):
        for record in batch.to_pylist():
            dataset_3_record = {v: None for v in fields_to_embed.values()}
            dataset_3_record['url'] = record['url__request_url']
            dataset_3_record['embedding_updated_at'] = current_time
            dataset_3_record['global_emb'] = None

            row_mappings: list[tuple[str, str]] = []
            global_texts: list[str] = []
            for src_field, out_col in fields_to_embed.items():
                value = record.get(src_field)
                if not value:
                    continue
                if isinstance(value, list):
                    value = ' | '.join(value)
                global_texts.append(value)
                hash_key = f'{row_counter}::{src_field}'
                pending_texts[hash_key] = value
                row_mappings.append((hash_key, out_col))

            if global_texts:
                global_hash_key = f'{row_counter}::global'
                pending_texts[global_hash_key] = ' '.join(global_texts)
                row_mappings.append((global_hash_key, 'global_emb'))

            pending_records.append(dataset_3_record)
            pending_mappings.append(row_mappings)
            row_counter += 1

            if len(pending_texts) >= embed_batch_size:
                embedded = embed_documents(pending_texts, embedding_client)
                for rec, mappings in zip(pending_records, pending_mappings, strict=True):
                    for hk, output_col in mappings:
                        emb = embedded.get(hk)
                        if emb is not None and len(emb) == EMBEDDING_DIMENSIONS:
                            rec[output_col] = emb
                yield pending_records
                pending_texts.clear()
                pending_records = []
                pending_mappings = []

    if pending_texts:
        embedded = embed_documents(pending_texts, embedding_client)
        for rec, mappings in zip(pending_records, pending_mappings, strict=True):
            for hk, output_col in mappings:
                emb = embedded.get(hk)
                if emb is not None and len(emb) == EMBEDDING_DIMENSIONS:
                    rec[output_col] = emb
    if pending_records:
        yield pending_records


def check_output(output_path: str):
    import polars as pl

    reader = pq.read_table(output_path)
    print(reader.schema)
    table = pl.from_arrow(reader)
    print(table)


if __name__ == '__main__':
    main()
    # check_output('tests/dataset_3.parquet')
