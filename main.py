import random
import json

import pandas as pd
import argparse

from util import get_argparser
from util import read_inputs
from util.date_generator import DateGenerator
from util.query_generator import QueryGenerator


TYPE_MAPPER = {
    'string': 'text',
    'number': 'number',
    'date': 'text',
    'bool': 'text'
}


def parse_option():
    parser = get_argparser()
    args = parser.parse_args()
    return args


def main(args):
    # read input csv
    tables, columns, dt_tp, dtcol_tp, query_t, where_t, agg_t = read_inputs(args)

    # creates datetime condition for both query, and question
    date_generator = DateGenerator(dt_tp, dtcol_tp, columns)
    # creates query and question for all columns and synonyms
    query_generator = QueryGenerator(columns, tables, where_t, query_t, agg_t, date_generator)

    query_list = []
    question_list = []
    for table in tables.table.unique():
        querys, questions = query_generator.get_query_list(table, args.num_labels)
        query_list.extend(querys)
        question_list.extend(questions)

    # create input data for schema
    tables_list = []
    column_names = [[-1, '*']]
    column_names_original = [[-1, '*']]
    column_types = ['text']
    table_names = []
    table_names_original = []

    for idx, trow in tables.iterrows():
        for i, row in columns[columns.table == trow['table']].iterrows():
            column_names.append([idx, row['column'].lower().replace('_', ' ')])
            column_names_original.append([idx, row['column']])
            column_types.append(TYPE_MAPPER[row['type']])
        table_names.append(trow['table_nat'][0])
        table_names_original.append(trow['table'])

    tables_list.append({
        "column_names": column_names,
        "column_names_original": column_names_original,
        "column_types": column_types,
        "db_id": "o_tpani_cem",
        "foreign_keys": [],
        "primary_keys": [],
        "table_names": table_names,
        "table_names_original": table_names_original
    })
    with open(args.result_tables_path, 'w') as f:
        json.dump(tables_list, f)

    # create input data for question and query
    labels = []
    for query, question in zip(query_list, question_list):
        labels.append({'question': question, 'query': query, 'db_id': 'o_tpani_cem'})
    with open(args.result_labels_path, 'w') as f:
        json.dump(labels, f, indent=2)


if __name__ == '__main__':
    args = parse_option()
    print('args: ', args)

    main(args)

