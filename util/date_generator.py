from datetime import datetime, timedelta
import random

RANGE_DELIMITER = ['~', 'and', 'to']
BASE_DELIMITER = '~'
QUERY_DELIMITER = 'AND'


class DateGenerator:

    def __init__(self, dt_temp, dtcol_temp):
        self.dt_temp = dt_temp
        self.dtcol_temp = dtcol_temp

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

    def get_date_condition(self, dt_col, force_range=False):
        col_format = self.dtcol_temp[self.dtcol_temp['column'] == dt_col].iloc[0]['column_format']
        col_type = self.dtcol_temp[self.dtcol_temp['column'] == dt_col].iloc[0]['type']
        dt_df = self.dt_temp[self.dt_temp.type == col_type].index

        # pick random format from nat formats which matches selected dt_col's type
        rpick = random.choice(dt_df)
        query_format = self.dt_temp.loc[rpick]['query_format']
        nat_format = self.dt_temp.loc[rpick]['nat_format']
        range = self.dt_temp.loc[rpick]['range']

        random_day = datetime.now() - timedelta(int(random.random() * 2 * 30 * 12), hours=int(random.random() * 24), minutes=int(random.random() * 60))

        # get range datetime condition
        if range or force_range:
            random_day2 = random_day + timedelta(days=int(random.random() * 10 + 1), hours=int(random.random() * 24), minutes=int(random.random() * 60))

            nat_delimiter = nat_format.find(BASE_DELIMITER)
            nat_range_dt1 = nat_format[:nat_delimiter]
            nat_range_dt2 = nat_format[nat_delimiter:]

            nat_range_dt1 = self._daymonth_formatter(random_day, nat_range_dt1)
            nat_range_dt2 = self._daymonth_formatter(random_day2, nat_range_dt2)
            date_nat = nat_range_dt1 + nat_range_dt2
            date_nat = date_nat.replace('~', random.choice(RANGE_DELIMITER))  # replace range delimiter randomly

            org_delimiter = query_format.find(QUERY_DELIMITER)
            org_range_dt1 = query_format[:org_delimiter]
            org_range_dt2 = query_format[org_delimiter:]
            org_range_dt1 = org_range_dt1.replace('{date_column}', dt_col).replace('{datetime}', datetime.strftime(random_day, col_format))
            org_range_dt2 = org_range_dt2.replace('{datetime}', datetime.strftime(random_day2, col_format))
            date_org = org_range_dt1 + org_range_dt2

        # get fixed datetime condition
        else:
            date_nat = self._daymonth_formatter(random_day, nat_format)
            date_org = query_format.replace('{date_column}', dt_col).replace('{datetime}', datetime.strftime(random_day, col_format))

        return date_org, date_nat
