import random

import pandas as pd
import argparse

from util.date_generator import DateGenerator
from util.query_generator import QueryGenerator


def parse_option():
    parser = argparse.ArgumentParser("")

    parser.add_argument('--columns_path', type=str, default="./input/columns.csv")
    parser.add_argument('--tables_path', type=str, default="./input/tables.csv")
    parser.add_argument('--query_template_path', type=str, default="./input/query_template.csv")
    parser.add_argument('--where_template_path', type=str, default="./input/where_template.csv")
    parser.add_argument('--datetime_template_path', type=str, default="./input/datetime_template.csv")
    parser.add_argument('--datecolumn_template_path', type=str, default="./input/datecolumn_template.csv")
    parser.add_argument('--agg_template_path', type=str, default="./input/agg_template.csv")

    args = parser.parse_args()

    return args


def main(args):
    tables = pd.read_csv(args.tables_path, header=0, converters={'table_nat': pd.eval})
    columns = pd.read_csv(args.columns_path, header=0, converters={'table_list': pd.eval, 'synonym_list': pd.eval, 'sample_list': pd.eval})
    columns['synonym_list'] = columns.apply(lambda x: x['synonym_list'] + [x['column']], axis=1)
    columns = columns.explode('table_list').rename(columns={'table_list': 'table'}).reset_index(drop=True)
    dt_tp = pd.read_csv(args.datetime_template_path, header=0)
    dtcol_tp = pd.read_csv(args.datecolumn_template_path, header=0)
    query_t = pd.read_csv(args.query_template_path, header=0)
    where_t = pd.read_csv(args.where_template_path, header=0)
    agg_t = pd.read_csv(args.agg_template_path, header=0, converters={'function_nat_list': pd.eval})

    # creates datetime condition for both query, and question
    date_generator = DateGenerator(dt_tp, dtcol_tp)
    # creates query and question for all columns and synonyms
    query_generator = QueryGenerator(columns, tables, where_t, query_t, date_generator)

    for i in range(5):
        print(date_generator.get_date_condition('dt', 'dt'))
    for i in range(5):
        print(date_generator.get_date_condition('etl_end_dtts', 'dt'))

    for table in tables.table.unique():
        query_list, question_list = query_generator.get_query_list(table)


if __name__ == '__main__':
    args = parse_option()
    print('args: ', args)

    main(args)

