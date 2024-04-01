# Text2SQL Query Label Maker
This is an application to automatically create natural language question and query from given database schema and question/query templates.

Goal is to easily add certain question format or new sql function syntax easily to label data.

I made this since there are hundreds and thousands of tables/columns and I can't waste my time on them creating thousands of label data for Text2SQL.

## Input files

- columns.csv : list of all columns from all the tables (columns: table_list, column, type, column_nat, sample_list, partition_key)
- tables.csv : list of all tables (columns: table, table_nat)
- query_template.csv : query format tempaltes, need to fill template with select, from and where (columns: type, question, query)
- datetime_template.csv : list of natural language and query in each date format (columns: query_format, nat_format, type, range)
- datecolumn_template.csv : list of date columns and their date formats (columns: column, column_format, type)
- agg_template.csv : list of aggregation functions (columns: function, function_nat_list)
- where_template.csv : list of all kinds of value comparing conditions (columns: where_nat, where,type)


## How to run
prepare input files  <br>
! input files **must** match original csv file's column names


run main.py
```python main.py```

## How to add table/column



## How to add datetime format


## How to add question/query syntax

