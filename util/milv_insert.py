import torch
import numpy as np
import json
from transformers import BertTokenizer, BertModel, AutoModel, AutoTokenizer
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

(py10) [jiyeon@testcw01 vectordb]$ cat milv_insert.py 
import torch
import numpy as np
import json
from transformers import BertTokenizer, BertModel, AutoModel, AutoTokenizer
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

model_name = "/data1/jiyeon/base-models/bge-m3"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

def average_pool(last_hidden_states: torch, attention_mask: torch) -> torch:
    last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
    return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]

def embed_sentence(sentence, model=model, tokenizer=tokenizer):
    inputs = tokenizer(sentence, return_tensors="pt")
    outputs = model(**inputs)
    # embeddings = outputs.last_hidden_state.mean(dim=1)  # Assuming mean pooling for simplicity
    embeddings = average_pool(outputs.last_hidden_state, inputs['attention_mask'])
    return embeddings.detach().numpy()[0]

doc_list = json.load(open('./json_data1.json'))

user_question_vector = [embed_sentence(voc['user_question']) for voc in doc_list]
insert_data = [
    [voc['user_question'] for voc in doc_list],
    [voc['answer'] for voc in doc_list],
    user_question_vector,
]
collection = Collection(collection_name)
mr = collection.insert(insert_data)

print(mr)
