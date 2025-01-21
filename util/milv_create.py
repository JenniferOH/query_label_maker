import numpy as np
import json
import pandas as pd

from pymilvus import connections, CollectionSchema, FieldSchema, DataType, db, utility, Collection, IndexType, MilvusClient

conn = connections.connect(
    alias="default",
    host="10.10.27.30",
    port="19530"
)
milvus_client = MilvusClient(uri="http://10.10.27.30:19530")
db.using_database("default")
r = utility.list_collections(timeout=None, using='default')
print(r)

collection_name = 'lots_test1'
dimension = 1024
fields = [
    FieldSchema(name="voc_id", dtype=DataType.INT64, is_primary=True, auto_id=True),
    FieldSchema(name="user_question", dtype=DataType.VARCHAR, max_length=2000),
    FieldSchema(name="answer", dtype=DataType.VARCHAR, max_length=2000),
    FieldSchema(name="user_question_vector", dtype=DataType.FLOAT_VECTOR, dim=dimension),
]

schema = CollectionSchema(
  fields=fields,
  description="voc user question",
  enable_dynamic_field=True
)

# Create the collection
collection = Collection(name=collection_name, schema=schema, shard_num=1)

index_params = {
  # "metric_type":"L2",
  "metric_type":"COSINE",
  "index_type":"IVF_FLAT",
  "params":{"nlist":1024}
}
# index_params = {"IVF_FLAT": {"metric_type": "L2", "params": {"nprobe": 10}}}
collection.create_index(
  field_name="user_question_vector",
  index_params=index_params
)

result = utility.index_building_progress(collection_name)
print(result)
