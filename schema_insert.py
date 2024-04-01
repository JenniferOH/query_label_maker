import ast
import random
import numpy as np
import pandas as pd

from util import get_argparser
from util import read_inputs
table = pd.read_csv('./input/tables.csv')


def parse_option():
    parser = get_argparser()

    parser.add_argument('--table_name', type=str, required=True)
    parser.add_argument('--table_nat_list', type=str, required=True)
    args = parser.parse_args()
    return args


def main(args):
    table_name = args.table_name
    table_nat_list = args.table_nat_list.split(',')
    tables, columns, dt_tp, dtcol_tp, query_t, where_t, agg_t = read_inputs(args)

    if table_name not in tables.table.unique():
        new_table = pd.DataFrame({'table': table_name, 'table_nat': table_nat_list})
        tables = pd.concat([tables, new_table])

    tables.to_csv(args.tables_path, index=False)
    print(tables)


if __name__ == '__main__':
    args = parse_option()
    print('args: ', args)

    main(args)
