import pandas as pd
import argparse


def get_argparser():
    parser = argparse.ArgumentParser("")

    parser.add_argument('--columns_path', type=str, default="./input/columns.csv")
    parser.add_argument('--tables_path', type=str, default="./input/tables.csv")
    parser.add_argument('--query_template_path', type=str, default="./input/query_template.csv")
    parser.add_argument('--where_template_path', type=str, default="./input/where_template.csv")
    parser.add_argument('--datetime_template_path', type=str, default="./input/datetime_template.csv")
    parser.add_argument('--datecolumn_template_path', type=str, default="./input/datecolumn_template.csv")
    parser.add_argument('--agg_template_path', type=str, default="./input/agg_template.csv")
    parser.add_argument('--result_tables_path', type=str, default="./result/tables.json")
    parser.add_argument('--result_labels_path', type=str, default="./result/labels.json")
    parser.add_argument('--random_seed', type=int, default=0)
    parser.add_argument('--num_labels', type=int, default=10)
    parser.add_argument('--shuffle', type=bool, default=True)

    # args = parser.parse_args()

    return parser


def read_inputs(args):
    tables = pd.read_csv(args.tables_path, header=0, converters={'table_nat': pd.eval})
    # tables = pd.read_csv(args.tables_path, header=0)
    columns = pd.read_csv(args.columns_path, header=0,
                          converters={'table_list': pd.eval, 'sample_list': pd.eval})
                          # converters={'table_list': pd.eval, 'column_nat': pd.eval, 'sample_list': pd.eval})
    # columns['column_nat'] = columns.apply(lambda x: x['column_nat'] + [x['column']], axis=1)
    columns = columns.explode('table_list').rename(columns={'table_list': 'table'}).reset_index(drop=True)
    dt_tp = pd.read_csv(args.datetime_template_path, header=0)
    dtcol_tp = pd.read_csv(args.datecolumn_template_path, header=0, converters={'table_list': pd.eval})
    dtcol_tp = dtcol_tp.explode('table_list').rename(columns={'table_list': 'table'}).reset_index(drop=True)
    query_t = pd.read_csv(args.query_template_path, header=0)
    if args.shuffle:
        query_t = query_t.sample(frac=1, random_state=0).reset_index(drop=True)
    where_t = pd.read_csv(args.where_template_path, header=0)
    agg_t = pd.read_csv(args.agg_template_path, header=0, converters={'function_nat_list': pd.eval})

    return tables, columns, dt_tp, dtcol_tp, query_t, where_t, agg_t