import random
import pandas as pd


class QueryGenerator:

    def __init__(self, col_df, table_df, where_temp, query_temp, date_generator):
        self.col_df = col_df
        self.table_df = table_df
        self.where_temp = where_temp
        self.query_temp = query_temp
        self.date_generator = date_generator

    def get_query_list(self, table):
        query_list = []
        question_list = []
        column_df = self.col_df[self.col_df.table == table]
        partition_column_df = column_df[column_df.partition_key == 1]
        assert len(partition_column_df) > 0, 'there is no partition key in table {}!'.format(table)
        assert 'date' in partition_column_df.type.unique(), 'table {} is missing date partition key!'.format(table)
        date_partition = partition_column_df[partition_column_df.type == 'date'].iloc[0].column
        col_count = len(column_df)
        print('\n>>>> table {}  columns: {}'.format(table, column_df.column.unique().tolist()))

        for i in range(5):
            template = self.query_temp.loc[i]
            question = template.question
            query = template.query
            max_cols = col_count if col_count < 10 else 10  # max select column count

            # pick random columns for 'select' part
            select_col_df = column_df.sample(random.randint(1, max_cols))
            select_column_nat = select_col_df.apply(lambda x: random.choice(x['synonym_list']), axis=1).tolist()
            select_column = select_col_df.column.tolist()

            question = question.replace('{select_column_nat}', ', '.join(select_column_nat))
            query = query.replace('{select_column}', ' ,  '.join(select_column)).replace('{table}', table)

            # pick random columns to use in 'where' part
            where_col_df = column_df.sample(random.randint(1, int(max_cols/2)))

            where_cond_nat_str = ''
            where_cond_str = ''
            for idx, row in where_col_df.iterrows():
                if where_cond_nat_str != '':
                    where_cond_nat_str += ' and '
                    where_cond_str += ' AND '
                col_type = row.type
                sample_list = row['sample_list']
                if col_type == 'date':
                    where_cond_str, where_cond_nat_str, where_cond_pk_str = self.date_generator.get_date_condition(row['column'], date_partition)
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

            # if i%2 == 0:
            #     date_org, date_nat = self.date_generator.get_date_condition('dt')
            #     where_cond_nat_str +=

            if 'date' in where_col_df.type.unique() and date_partition not in where_col_df['column'].values:
                # if date column was picked as 'where' column, but date type partition key is missing, must add it
                where_cond_str = where_cond_pk_str + ' AND ' + where_cond_str

            question = question.replace('{where_condition_nat}', where_cond_nat_str)\
                    .replace('{table_nat}', random.choice(self.table_df[self.table_df.table == table].table_nat.values[0]))
            query = query.replace('{where_condition}', where_cond_str)

            print(question)
            print(query)
            query_list.append(query)
            question_list.append(question)

        return query_list, question_list


