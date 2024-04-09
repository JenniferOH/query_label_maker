import random
import json

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

    for idx, row in tables.groupby(['database', 'table']).first().reset_index().iterrows():
        db = row['database']
        table = row['table']
        querys, questions = query_generator.get_query_list(db, table, args.num_labels)
        query_list.extend(querys)
        question_list.extend(questions)

        if idx < 10:
            print('\n>>>> table {}.{}  '.format(db, table))
            for qr, qs in zip(querys, questions):
                print(qs, qr)

    # create input data for schema
    tables_list = []
    column_names = ['*']
    column_names_original = ['*']
    column_types = ['text']
    table_names = []
    table_names_original = []

    for idx, trow in tables.iterrows():
        table_names.append(trow['table_nat'][0])
        table_names_original.append(trow['table'])

    for i, row in columns.groupby('column').first().reset_index().iterrows():
        column_names.append(row['column'].lower().replace('_', ' '))
        column_names_original.append(row['column'])

    tables_list.append({
        "column_names": column_names,
        "column_names_original": column_names_original,
        # "column_types": column_types,
        "db_id": "infra_dt",
        # "foreign_keys": [],
        # "primary_keys": [],
        "table_names": table_names,
        "table_names_original": table_names_original
    })

    # print('@@@ column_names: ', column_names[:10])
    with open(args.result_tables_path, 'w') as f:
        json.dump(tables_list, f)

    # create input data for question and query
    labels = []
    for query, question in zip(query_list, question_list):
        labels.append({'question': question, 'query': query, 'db_id': 'infra_dt'})
    with open(args.result_labels_path, 'w') as f:
        json.dump(labels, f, indent=2)


if __name__ == '__main__':
    args = parse_option()
    print('args: ', args)

    main(args)

