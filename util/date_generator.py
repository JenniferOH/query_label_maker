from datetime import datetime, timedelta
import random
import calendar

RANGE_DELIMITER = ['~', 'and', 'to']
BASE_DELIMITER = '~'
QUERY_DELIMITER = 'AND'


class DateGenerator:

    def __init__(self, dt_temp, dtcol_temp, columns):
        self.dt_temp = dt_temp
        self.dtcol_temp = dtcol_temp
        self.columns = columns

    # randomly, reformat literal date, remove zero padding in day, add suffix(th/st/nd/rd) to day
    def _day_formatter(self, random_day, formatter):
        if random_day.microsecond % 2 == 0:
            return datetime.strftime(random_day, formatter)
        if '%B' in formatter and '%d' in formatter:
            day = int(datetime.strftime(random_day, '%d'))
            if 4 <= day <= 20 or 24 <= day <= 30:
                suffix = "th"
            else:
                suffix = ["st", "nd", "rd"][day % 10 - 1]
            return datetime.strftime(random_day, formatter.replace('%d', '{}{}'.format(day, suffix)))
        else:
            return datetime.strftime(random_day, formatter)

    # randomly, remove zero padding on day and month, only for date (not time)
    def _daymonth_formatter(self, random_day, formatter):
        ff = formatter
        if random_day.second % 3 == 0:
            pass
        elif '%H' not in formatter and '%m' in formatter and '%d' in formatter:
            day = int(datetime.strftime(random_day, '%d'))
            month = int(datetime.strftime(random_day, '%m'))
            ff = formatter.replace('%d', '{}'.format(day)).replace('%m', '{}'.format(month))

        return self._day_formatter(random_day, ff)

    def get_date_condition(self, table, dt_col, dt_part_key, force_range=False):
        target_dtcol = self.dtcol_temp[self.dtcol_temp.table == table]
        assert len(target_dtcol) > 0, "column '{}' in table '{}' must be in datecolumn template!".format(dt_col, table)
        target_dtcol = target_dtcol[target_dtcol['column'] == dt_col]
        assert len(target_dtcol) > 0, "column '{}' in column '{}' must be in datecolumn template!".format(dt_col, table)
        target_col = self.columns[self.columns['column'] == dt_col]
        assert len(target_col) > 0, "column '{}' must be in column template!".format(dt_part_key)
        parkey_dtcol = self.dtcol_temp[self.dtcol_temp['column'] == dt_part_key]
        assert len(parkey_dtcol) > 0, "column '{}'(partition key) must be in datecolumn template!".format(dt_part_key)

        col_format = target_dtcol.iloc[0]['column_format']
        col_type = target_dtcol.iloc[0]['type']
        pk_col_format = parkey_dtcol.iloc[0]['column_format']
        pk_col_type = parkey_dtcol.iloc[0]['type']
        dt_df = self.dt_temp[self.dt_temp.type == col_type].index
        col_nat_list = target_col.iloc[0]['synonym_list']
        is_partition = target_col.iloc[0]['partition_key']

        # pick random format from nat formats which matches selected dt_col's type
        rpick = random.choice(dt_df)
        query_format = self.dt_temp.loc[rpick]['query_format']
        nat_format = self.dt_temp.loc[rpick]['nat_format']
        range = self.dt_temp.loc[rpick]['range']
        # get query format for date partition key with same type and range
        pk_query_format = self.dt_temp[(self.dt_temp.type == pk_col_type) & (self.dt_temp.range == range)].iloc[0]['query_format']

        random_day = datetime.now() - timedelta(int(random.random() * 2 * 30 * 12), hours=int(random.random() * 24), minutes=int(random.random() * 60))

        # get range datetime condition
        if range > 0 or force_range:
            if range == 1:
                random_day2 = random_day + timedelta(days=int(random.random() * 10 + 1), hours=int(random.random() * 24), minutes=int(random.random() * 60))
            else:
                random_day = datetime(random_day.year, random_day.month, 1)
                random_day2 = datetime(random_day.year, random_day.month, calendar.monthrange(random_day.year, random_day.month)[1])

            # construct date range condition nat question
            nat_delimiter = nat_format.find(BASE_DELIMITER)
            if nat_delimiter > 0:
                nat_range_dt1 = nat_format[:nat_delimiter]
                nat_range_dt2 = nat_format[nat_delimiter:]
            else:
                nat_range_dt1 = nat_format
                nat_range_dt2 = nat_format
            nat_range_dt1 = self._daymonth_formatter(random_day, nat_range_dt1)
            nat_range_dt2 = self._daymonth_formatter(random_day2, nat_range_dt2)
            if range == 1:
                date_nat = nat_range_dt1 + nat_range_dt2
            else:
                date_nat = nat_range_dt1
            if is_partition:
                date_nat = date_nat.replace('{date_column_nat}', '')
            else:
                date_nat = date_nat.replace('{date_column_nat}', random.choice(col_nat_list))
            date_nat = date_nat.replace('~', random.choice(RANGE_DELIMITER))  # replace range delimiter randomly

            # construct date range condition query
            org_delimiter = query_format.find(QUERY_DELIMITER)
            org_format_dt1 = query_format[:org_delimiter]
            org_format_dt2 = query_format[org_delimiter:]
            org_range_dt1 = org_format_dt1.replace('{date_column}', dt_col).replace('{datetime}', datetime.strftime(random_day, col_format))
            org_range_dt2 = org_format_dt2.replace('{datetime}', datetime.strftime(random_day2, col_format))
            date_org = org_range_dt1 + org_range_dt2

            # construct date range condition query for partition key
            pk_delimiter = pk_query_format.find(QUERY_DELIMITER)
            pk_format_dt1 = pk_query_format[:pk_delimiter]
            pk_format_dt2 = pk_query_format[pk_delimiter:]
            pk_range_dt1 = pk_format_dt1.replace('{date_column}', dt_part_key).replace('{datetime}', datetime.strftime(random_day, pk_col_format))
            pk_range_dt2 = pk_format_dt2.replace('{datetime}', datetime.strftime(random_day2, pk_col_format))
            partition_org = pk_range_dt1 + pk_range_dt2

        # get fixed datetime condition
        else:
            date_col_nat = random.choice(col_nat_list)
            if is_partition:
                date_col_nat = ''
            date_nat = self._daymonth_formatter(random_day, nat_format).replace('{date_column_nat}', date_col_nat).replace('{rand_hour}', str(random_day.hour))
            date_org = query_format.replace('{date_column}', dt_col).replace('{datetime}', datetime.strftime(random_day, col_format)).replace('{rand_hour}', str(random_day.hour))
            partition_org = pk_query_format.replace('{date_column}', dt_part_key).replace('{datetime}', datetime.strftime(random_day, pk_col_format))

        return date_org, date_nat, partition_org

