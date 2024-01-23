import random
import pandas as pd

WHERE_DELIMETER = ['with ', 'where ', 'which ']
WHERE_DELIMETER_BOOL = ['which ', 'that ']
COUNT_FORMAT = 'COUNT ( DISTINCT {count_column} )'
AGG_FORMAT = '{agg_function} ( {agg_column} )'
AND_NAT = [' and ', ' , ']
TABLE_TERM = ['daily', 'daily basis', 'hourly basis', 'hourly', 'weekly', 'monthly']


class QueryGenerator:

    def __init__(self, col_df, table_df, where_temp, query_temp, agg_temp, date_generator):
        self.col_df = col_df[col_df.partition_key > -1]
        self.table_df = table_df
        self.where_temp = where_temp
        self.query_temp = query_temp
        self.agg_temp = agg_temp
        self.date_generator = date_generator

    def get_query_list(self, table, num_labels):
        query_list = []
        question_list = []
        column_df = self.col_df[self.col_df.table == table]
        numeric_column_df = column_df[column_df.type == 'number']
        partition_column_df = column_df[column_df.partition_key == 1]
        assert len(partition_column_df) > 0, 'there is no partition key in table {}!'.format(table)
        assert 'date' in partition_column_df.type.unique(), 'table {} is missing date partition key!'.format(table)
        date_partition = partition_column_df[partition_column_df.type == 'date'].iloc[0].column
        col_count = len(column_df)
        num_col_count = len(numeric_column_df)
        max_cols = col_count if col_count < 5 else 5  # max select column count
        max_num_cols = num_col_count if num_col_count < 3 else 3
        print('\n>>>> table {}  columns: {}'.format(table, column_df.column.unique().tolist()))

        for i in range(num_labels):
            template = self.query_temp.loc[i%len(self.query_temp)]
            template_type = template.type.astype(str)
            question = template.question
            query = template.query

            # pick random columns for 'select' part
            select_col_df = column_df.sample(random.randint(1, max_cols-1))
            select_column_nat_list = select_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist()
            select_column_list = select_col_df.column.tolist()

            # if template_type == '0':  # simple query
            question = question.replace('{select_column_nat}', ', '.join(select_column_nat_list))
            query = query.replace('{select_column}', ' ,  '.join(select_column_list)).replace('{table}', table)

            if '1' in template_type:    # count query
                count_col_df = column_df[column_df.type == 'string'].sample(random.randint(1, 2))
                count_column_nat_list = count_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist()
                count_column_list = count_col_df.column.tolist()
                question = question.replace('{count_column_nat}', ', '.join(count_column_nat_list))
                count_format = ''
                for count_col in count_column_list:
                    count_format += COUNT_FORMAT.replace('{count_column}', count_col)
                    if count_column_list[-1] != count_col:
                        count_format += ' ,  '
                query = query.replace('{count_format}', count_format)

            if '2' in template_type:    # aggregation(group by) query
                agg_df = self.agg_temp.sample(random.randint(1, 3))
                agg_func_nat_list = agg_df.apply(lambda x: random.choice(x['function_nat_list']), axis=1).tolist()
                agg_func_list = agg_df.function.tolist()
                agg_function_nat = ', '.join(agg_func_nat_list)

                # pick numeric columns for aggregation
                agg_func_col_df = numeric_column_df.sample(random.randint(1, 2))
                agg_function_column_nat = ', '.join(agg_func_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist())
                agg_function_column_list = agg_func_col_df.column.tolist()

                # re-pick columns which are not used for aggretation
                groupby_col_df = column_df[~column_df.column.isin(agg_func_col_df.column)].sample(random.randint(1, max_num_cols - len(agg_func_col_df) + 1))
                groupby_column_nat_list = groupby_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist()
                groupby_column_list = groupby_col_df.column.tolist()

                # make aggregation function calls with columns
                agg_function_column = ''
                for func in agg_func_list:
                    agg_function_column += ' ,  '.join([AGG_FORMAT.replace('{agg_function}', func).replace('{agg_column}', col) for col in agg_function_column_list])
                    if agg_func_list[-1] != func:
                        agg_function_column += ' ,  '

                question = question.replace('{agg_function_nat}', agg_function_nat)
                question = question.replace('{agg_function_column_nat}', agg_function_column_nat)
                query = query.replace('{agg_function_column}', agg_function_column)
                question = question.replace('{groupby_column_nat}', ', '.join(groupby_column_nat_list))
                query = query.replace('{groupby_column}', ' ,  '.join(groupby_column_list))

            if '3' in template_type:    # order by query
                limit_count = random.randint(1, 100)

                # re-pick columns which are not used for select
                orderby_col_df = column_df[~column_df.column.isin(select_col_df)].sample(random.randint(1, max_cols - len(select_col_df)))
                orderby_column_nat_list = orderby_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist()
                orderby_column_list = orderby_col_df.column.tolist()

                question = question.replace('{orderby_column_nat}', ', '.join(orderby_column_nat_list))
                query = query.replace('{orderby_column}', ' ,  '.join(orderby_column_list))
                question = question.replace('{limit_count}', str(limit_count))
                query = query.replace('{limit_count}',  str(limit_count))

            # pick random columns to use in 'where' part
            where_col_df = column_df.sample(random.randint(1, int(max_cols/2)))
            # if there is no date, add one
            if len(where_col_df[where_col_df.type == 'date']) < 1:
                where_col_df = pd.concat([where_col_df, column_df[column_df.type == 'date'].sample(1)])
            # if there is date type, pick 1 and put date column to the end
            if len(where_col_df[where_col_df.type == 'date']) > 0:
                where_col_df = pd.concat([where_col_df[where_col_df.type != 'date'], where_col_df[where_col_df.type == 'date'][:1]])
            where_cond_nat_str = ''
            where_cond_str = ''
            for idx, row in where_col_df.iterrows():
                if where_cond_nat_str != '':
                    where_cond_nat_str += AND_NAT[i%2]
                    where_cond_str += ' AND '
                col_type = row.type
                sample_list = row['sample_list']

                if col_type == 'date':
                    org_str, nat_str, where_cond_pk_str = self.date_generator.get_date_condition(table, row['column'], date_partition)
                    # remove 'and' in where condition if column is date partition key
                    if where_cond_nat_str != '' and row['partition_key'] == 1:
                        where_cond_nat_str = where_cond_nat_str[:-len(AND_NAT[i%2])]
                    where_cond_str += org_str
                    where_cond_nat_str += nat_str
                    where_deli = random.choice(WHERE_DELIMETER)
                elif col_type == 'bool' and len(where_col_df) < 2:
                    where_template = self.where_temp[self.where_temp.type == 'bool'].sample(1)
                    where_cond_nat_str += where_template.where_nat.values[0]\
                        .replace('{column_nat}', row['synonym_list'][0])
                    where_cond_str += where_template['where'].values[0]\
                        .replace('{column}', row['column'])
                    where_deli = random.choice(WHERE_DELIMETER_BOOL)
                else:
                    if col_type == 'bool':
                        col_type = 'string'
                        synonym_list = row['synonym_list'][1:]
                        value_list = 'y'
                    elif col_type == 'number':
                        value_list = ', '.join([str(s) for s in sample_list])
                        synonym_list = row['synonym_list']
                    else:
                        value_list = ', '.join(["'{}'".format(s) for s in sample_list])
                        synonym_list = row['synonym_list']
                    where_template = self.where_temp[(self.where_temp.type == col_type) | (self.where_temp.type == 'all')].sample(1)
                    sample_value = str(random.choice(sample_list))
                    where_cond_nat_str += where_template.where_nat.values[0]\
                        .replace('{column_nat}', random.choice(synonym_list))\
                        .replace('{value}', sample_value)\
                        .replace('{value_list}', value_list)\
                        .replace('{value_range1}', str(min(sample_list)))\
                        .replace('{value_range2}', str(max(sample_list)))
                    where_cond_str += where_template['where'].values[0]\
                        .replace('{column}', row['column'])\
                        .replace('{value}', sample_value)\
                        .replace('{value_list}', value_list)\
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

            ### TODO: tempporary added for table nat
            if [i%2] == 0:
                question = question.replace('{where_condition_nat}', where_cond_nat_str)\
                        .replace('{where_delimiter}', where_deli)\
                        .replace('{table_nat}', random.choice(TABLE_TERM) + ' ' + random.choice(self.table_df[self.table_df.table == table].table_nat.values[0]))
            else:
                question = question.replace('{where_condition_nat}', where_cond_nat_str)\
                        .replace('{where_delimiter}', where_deli)\
                        .replace('{table_nat}', random.choice(self.table_df[self.table_df.table == table].table_nat.values[0]) +  ' ' + random.choice(TABLE_TERM))

            query = query.replace('{where_condition}', where_cond_str)

            print(question)
            print(query)
            query_list.append(query)
            question_list.append(question)

        return query_list, question_list


