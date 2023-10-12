import random
import pandas as pd

WHERE_DELIMETER = ['with ', 'where ', 'which ']
COUNT_FORMAT = 'COUNT ( DISTINCT {select_column} )'
AGG_FORMAT = '{agg_function} ( {agg_column} )'


class QueryGenerator:

    def __init__(self, col_df, table_df, where_temp, query_temp, agg_temp, date_generator):
        self.col_df = col_df
        self.table_df = table_df
        self.where_temp = where_temp
        self.query_temp = query_temp
        self.agg_temp = agg_temp
        self.date_generator = date_generator

    def get_query_list(self, table):
        query_list = []
        question_list = []
        column_df = self.col_df[self.col_df.table == table]
        numeric_column_df = column_df[column_df.type == 'number']
        partition_column_df = column_df[column_df.partition_key == 1]
        assert len(partition_column_df) > 0, 'there is no partition key in table {}!'.format(table)
        assert 'date' in partition_column_df.type.unique(), 'table {} is missing date partition key!'.format(table)
        date_partition = partition_column_df[partition_column_df.type == 'date'].iloc[0].column
        col_count = len(column_df)
        print('\n>>>> table {}  columns: {}'.format(table, column_df.column.unique().tolist()))

        for i in range(50):
            template = self.query_temp.loc[i%8]
            template_type = template.type
            question = template.question
            query = template.query
            max_cols = col_count if col_count < 10 else 10  # max select column count

            # pick random columns for 'select' part
            select_col_df = column_df.sample(random.randint(1, max_cols))
            select_column_nat_list = select_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist()
            select_column_list = select_col_df.column.tolist()

            if template_type == 0:  # simple query
                question = question.replace('{select_column_nat}', ', '.join(select_column_nat_list))
                query = query.replace('{select_column}', ' ,  '.join(select_column_list)).replace('{table}', table)

            elif template_type == 1:    # count query
                question = question.replace('{select_column_nat}', ', '.join(select_column_nat_list))
                count_format = ''
                for select_column in select_column_list:
                    count_format += COUNT_FORMAT.replace('{select_column}', select_column)
                    if select_column_list[-1] != select_column:
                        count_format += ', '
                query = query.replace('{count_format}', count_format).replace('{table}', table)

            elif template_type == 2:    # aggregation
                # "show {agg_function_nat} of {agg_function_column_nat} for each {select_column_nat} {where_delimiter}{where_condition_nat}","SELECT {select_column}, {agg_function_column} FROM {table} WHERE {where_condition} GROUP BY {select_column}"
                agg_df = self.agg_temp.sample(random.randint(1, 3))
                agg_func_nat_list = agg_df.apply(lambda x: random.choice(x['function_nat_list']), axis=1).tolist()
                agg_func_list = agg_df.function.tolist()
                agg_function_nat = ', '.join(agg_func_nat_list)

                # pick numeric columns for aggregation
                agg_func_col_df = numeric_column_df.sample(random.randint(1, len(numeric_column_df)))
                agg_function_column_nat = ', '.join(agg_func_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist())

                # re-pick select columns which are not used for aggretation
                select_col_df = column_df[~column_df.column.isin(agg_func_col_df)].sample(random.randint(1, max_cols - len(numeric_column_df)))
                select_column_nat_list = select_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist()
                select_column_list = select_col_df.column.tolist()

                agg_function_column = ''
                for func in agg_func_list:
                    agg_function_column += ', '.join([AGG_FORMAT.replace('{agg_function}', func).replace('{agg_column}', col) for col in select_column_list])
                    if agg_func_list[-1] != func:
                        agg_function_column += ', '

                question = question.replace('{agg_function_nat}', agg_function_nat)
                question = question.replace('{agg_function_column_nat}', agg_function_column_nat)
                query = query.replace('{agg_function_column}', agg_function_column)
                question = question.replace('{select_column_nat}', ', '.join(select_column_nat_list))
                query = query.replace('{select_column}', ' ,  '.join(select_column_list)).replace('{table}', table)



            # pick random columns to use in 'where' part
            where_col_df = column_df.sample(random.randint(1, int(max_cols/2)))
            # put date column to the end
            where_col_df = pd.concat([where_col_df[where_col_df.type != 'date'], where_col_df[where_col_df.type == 'date']])
            where_cond_nat_str = ''
            where_cond_str = ''
            for idx, row in where_col_df.iterrows():
                if where_cond_nat_str != '':
                    where_cond_nat_str += ' and '
                    where_cond_str += ' AND '
                col_type = row.type
                sample_list = row['sample_list']
                if col_type == 'date':
                    org_str, nat_str, where_cond_pk_str = self.date_generator.get_date_condition(row['column'], date_partition)
                    # remove 'and' in where condition if column is partition key
                    if where_cond_nat_str != '' and row['partition_key'] == 1:
                        where_cond_nat_str = where_cond_nat_str[:-5]
                    where_cond_str += org_str
                    where_cond_nat_str += nat_str
                else:
                    where_template = self.where_temp[(self.where_temp.type == col_type) | (self.where_temp.type == 'all')].sample(1)
                    sample_value = str(random.choice(sample_list))
                    where_cond_nat_str += where_template.where_nat.values[0]\
                        .replace('{column_nat}', random.choice(row['synonym_list']))\
                        .replace('{value}', sample_value)\
                        .replace('{value_list}', ', '.join([str(s) for s in sample_list]))\
                        .replace('{value_range1}', str(min(sample_list)))\
                        .replace('{value_range2}', str(max(sample_list)))
                    where_cond_str += where_template['where'].values[0]\
                        .replace('{column}', row['column'])\
                        .replace('{value}', sample_value)\
                        .replace('{value_list}', str(sample_list))\
                        .replace('{value_range1}', str(min(sample_list)))\
                        .replace('{value_range2}', str(max(sample_list)))

            where_deli = random.choice(WHERE_DELIMETER)
            if 'date' in where_col_df.type.unique():
                # if date column was picked as 'where' column, but date type partition key is missing, must add it
                if date_partition not in where_col_df['column'].values:
                    where_cond_str = where_cond_pk_str + ' AND ' + where_cond_str
                # if date partition key is the only 'where' condition column, no need where delimiter
                elif len(where_col_df) == 1:
                    where_deli = ''

            question = question.replace('{where_condition_nat}', where_cond_nat_str)\
                    .replace('{where_delimiter}', where_deli)\
                    .replace('{table_nat}', random.choice(self.table_df[self.table_df.table == table].table_nat.values[0]))
            query = query.replace('{where_condition}', where_cond_str)

            print(question)
            print(query)
            query_list.append(query)
            question_list.append(question)

        return query_list, question_list


