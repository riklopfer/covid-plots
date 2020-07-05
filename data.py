import logging
import os
from datetime import datetime

import pandas as pd
import requests

DATA_DIR = "/tmp/covid-testing"

VALID_STATES = {
    'ak', 'al', 'ar', 'as', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga',
    'gu', 'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 'la', 'ma', 'md', 'me',
    'mi', 'mn', 'mo', 'mp', 'ms', 'mt', 'nc', 'nd', 'ne', 'nh', 'nj', 'nm',
    'nv', 'ny', 'oh', 'ok', 'or', 'pa', 'pr', 'ri', 'sc', 'sd', 'tn', 'tx',
    'ut', 'va', 'vi', 'vt', 'wa', 'wi', 'wv', 'wy',
}
ROLLING_WINDOW_TEST_SUFFIX = 'DayRollingTestRate'

_REQUIRED_COLUMNS = {'date'}


class DataSource(object):
    def __init__(self, column_map: dict, csv_date_column: str):
        missing_columns = _REQUIRED_COLUMNS - column_map.keys()
        if missing_columns:
            raise ValueError("Missing required column(s): '{}'".
                             format(missing_columns))
        if not csv_date_column:
            raise ValueError("Must provide date_column")

        self.date_column = csv_date_column
        self.column_map = column_map

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

    def dl_csv_data(self, target: str) -> str:
        # TODO refactor these implementations... i see some common code there.
        #  Also need to clean up after yourself.
        raise NotImplementedError("Check subclasses")

    def _maybe_add_test_rates(self, df, window):
        required_columns = {'totalTestResultsIncrease',
                            'positiveTestResultsIncrease'}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            self.logger.debug("Missing some columns there bud... {}".
                              format(missing_columns))
            return

        test_data = df[
            ['totalTestResultsIncrease', 'positiveTestResultsIncrease']]
        totals = test_data.rolling(window).sum()
        test_rates = (totals['positiveTestResultsIncrease'] /
                      totals['totalTestResultsIncrease'])
        df[str(window) + ROLLING_WINDOW_TEST_SUFFIX] = test_rates

    def _load_data_frame(self, target, window, start_date, end_date):
        csv_path = self.dl_csv_data(target)

        df = pd.read_csv(csv_path, parse_dates=[self.date_column])
        # map column names
        df.rename(columns=self.column_map, inplace=True)
        # oldest to newest
        # df = df.reindex(index=df.index[::-1])
        df.sort_values('date', inplace=True)
        #
        # Add rolling averages

        # Test Rates
        self._maybe_add_test_rates(df, window)

        # filter start
        if start_date is not None:
            df = df[df.date >= start_date]
        # filter end
        if end_date is not None:
            df = df[df.date <= end_date]

        return df

    def load_state_df(self, state, window, start_date=None,
                      end_date=None):
        if state not in VALID_STATES:
            raise ValueError("Invalid state abbreviation '{}'".format(state))

        return self._load_data_frame(state, window, start_date, end_date)

    def load_usa_df(self, window, start_date=None, end_date=None):
        df = self._load_data_frame('us', window, start_date, end_date)
        df['state'] = 'USA'
        return df


class CovidTracking(DataSource):
    def __init__(self):
        column_map = {
            'date': 'date',
            'totalTestResultsIncrease': 'totalTestResultsIncrease',
            'positiveIncrease': 'positiveTestResultsIncrease'
        }
        csv_date_column = 'date'
        super(CovidTracking, self).__init__(column_map, csv_date_column)

    def dl_csv_data(self, target: str) -> str:
        """
        https://covidtracking.com/api

        These are the columns of the CSV..

        ['date', 'state', 'positive', 'negative', 'pending',
           'hospitalizedCurrently', 'hospitalizedCumulative', 'inIcuCurrently',
           'inIcuCumulative', 'onVentilatorCurrently', 'onVentilatorCumulative',
           'recovered', 'dataQualityGrade', 'lastUpdateEt', 'dateModified',
           'checkTimeEt', 'death', 'hospitalized', 'dateChecked',
           'totalTestsViral', 'positiveTestsViral', 'negativeTestsViral',
           'positiveCasesViral', 'fips', 'positiveIncrease', 'negativeIncrease',
           'total', 'totalTestResults', 'totalTestResultsIncrease', 'posNeg',
           'deathIncrease', 'hospitalizedIncrease', 'hash', 'commercialScore',
           'negativeRegularScore', 'negativeScore', 'positiveScore', 'score',
           'grade']
        """
        target = target.lower()
        # this doesn't have county-level testing data
        out_dir = os.path.join(DATA_DIR, 'covidtracking', target)
        now_hour = datetime.utcnow().strftime('%Y-%m-%d:%H')
        out_path = os.path.join(out_dir, f'{now_hour}.daily.csv')
        # update once per hour?
        if os.path.exists(out_path):
            return out_path

        if target == 'us':
            url = f'https://covidtracking.com/api/v1/us/daily.csv'
        else:
            url = f'https://covidtracking.com/api/v1/states/{target}/daily.csv'

        r = requests.get(url)
        if r.status_code != 200:
            raise ValueError("Received bad status code {}\n{}".
                             format(r.status_code, r.text))
        assert r.status_code == 200

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        with open(out_path, 'w') as ofp:
            ofp.write(r.text)
        return out_path


class NyTimes(DataSource):
    def __init__(self):
        column_map = {
            'date': 'date',
            'state': 'state',
            'county': 'county',
            'cases': 'positiveTestResultsIncrease'
        }
        csv_date_column = 'date'
        super(NyTimes, self).__init__(column_map, csv_date_column)

    def dl_csv_data(self, target: str) -> str:
        # this doesn't have county-level testing data
        out_dir = os.path.join(DATA_DIR, 'nytimes')
        now_hour = datetime.utcnow().strftime('%Y-%m-%d:%H')
        out_path = os.path.join(out_dir, f'{now_hour}.daily.csv')
        # update once per hour?
        if os.path.exists(out_path):
            return out_path

        url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

        r = requests.get(url)
        if r.status_code != 200:
            raise ValueError("Received bad status code {}\n{}".
                             format(r.status_code, r.text))
        assert r.status_code == 200

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        with open(out_path, 'w') as ofp:
            ofp.write(r.text)
        return out_path
