import torch
import numpy as np
import json
from transformers import BertTokenizer, BertModel, AutoModel, AutoTokenizer
import pandas as pd
import sys
from pymilvus import connections, CollectionSchema, FieldSchema, DataType, db, utility, Collection, IndexType, MilvusClient

conn = connections.connect(
    alias="default",
    host="10.10.27.30",
    port="19530"
)
milvus_client = MilvusClient(uri="http://10.10.27.30:19530")
db.using_database("default")
r = utility.list_collections(timeout=None, using='default')

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

print('question: ', sys.argv[1])
query_vector = embed_sentence(sys.argv[1])
collection = Collection(collection_name)
collection.load()

search_params = {
    "metric_type": "COSINE",
    "offset": 0,
    "ignore_growing": False,
    "params": {"nprobe": 128}
}

results = milvus_client.search(
    collection_name=collection_name,
    data=[query_vector],
    search_param=search_params,
    consistency_level="Strong",
    output_fields=['user_question', 'answer']
)

print('\nresult: ')
for r in results:
    print(r)
