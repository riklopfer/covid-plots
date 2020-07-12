import logging
import os
from abc import ABC
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


def _dl_csv(url, data_source, target):
    target = target.lower()
    # this doesn't have county-level testing data
    out_dir = os.path.join(DATA_DIR, data_source, target)
    now_hour = datetime.utcnow().strftime('%Y-%m-%d:%H')
    out_path = os.path.join(out_dir, f'{now_hour}.daily.csv')
    # update once per hour?
    if os.path.exists(out_path):
        return out_path

    r = requests.get(url)
    if r.status_code != 200:
        raise ValueError("Received bad status code {}\n{}".
                         format(r.status_code, r.text))

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    with open(out_path, 'w') as ofp:
        ofp.write(r.text)
    return out_path


def convert_to_deltas(df):
    cumulative = df.groupby('date').sum()
    # insert one at the top with zeros
    before_time = pd.DataFrame(
        data={
            'date': [pd.Timestamp(0, unit='s')],
            'cases': [0],
            'deaths': [0]
        },
    ).set_index('date')
    delta = before_time.append(cumulative).diff().dropna()
    return delta


class DataUnavailableException(Exception):
    def __init__(self, *args):
        super(DataUnavailableException, self).__init__(*args)


class DailyData(object):
    POSITIVE_CASE_COL = 'cases'
    TEST_TOTAL_COL = 'tests'
    DEATHS_COL = 'deaths'

    def get_df(self) -> pd.DataFrame:
        """Returns a conformant data frame"""
        raise NotImplementedError

    def with_moving_averages(self, window) -> pd.DataFrame:
        df = self.get_df()

        # First find rolling means
        roller = df.rolling(window)
        means = roller.mean()
        df = df.join(means, rsuffix='_{}day-avg'.format(window))

        if DailyData.TEST_TOTAL_COL in df.columns:
            totals = roller.sum()
            test_rates = (totals[DailyData.POSITIVE_CASE_COL] /
                          totals[DailyData.TEST_TOTAL_COL])
            df['test-rate_{}day-avg'.format(window)] = test_rates

        return df


class _StateData(DailyData, ABC):

    def get_county_data(self, county_str) -> DailyData:
        raise NotImplementedError("County data unavailable")


class _NationalData(DailyData, ABC):

    def get_state_data(self, state_str) -> _StateData:
        raise NotImplementedError("State data not available")


class AggCountyData(DailyData):
    def __init__(self, df: pd.DataFrame):
        assert len(df[['state', 'county']].drop_duplicates()) == 1
        self.df = df

    def get_df(self) -> pd.DataFrame:
        deltas = convert_to_deltas(self.df)
        return deltas


class StateData(_StateData):

    def __init__(self, df: pd.DataFrame,
                 is_aggregate: bool):
        # assert len(df['state'].unique()) == 1
        self.df = df
        self.is_aggregate = is_aggregate

    def get_df(self) -> pd.DataFrame:
        if self.is_aggregate:
            return convert_to_deltas(self.df)
        else:
            return self.df

    def get_county_data(self, county_str) -> DailyData:
        if 'county' not in self.df.columns:
            raise DataUnavailableException("County data not available.")

        county_df = self.df[self.df.county == county_str]
        if county_df.empty:
            raise ValueError("Invalid state {} choose from {}".
                             format(county_df,
                                    self.df.county.unique()))
        return AggCountyData(county_df)


class NyTimesData(_NationalData):
    def __init__(self):
        # download data and create initial data frame
        csv_path = _dl_csv(
            "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv",
            'nytimes', 'us-counties'
        )
        """
        ['date', 'county', 'state', 'fips', 'cases', 'deaths']
        """
        self.df = pd.read_csv(csv_path, parse_dates=['date'],
                              usecols=['date', 'county', 'state', 'cases',
                                       'deaths'])
        # No mapping required
        self.df.sort_values('date', inplace=True)

    def get_state_data(self, state_str) -> StateData:
        state_df = self.df[self.df.state == state_str]
        if state_df.empty:
            raise ValueError("Invalid state {} choose from {}".
                             format(state_str,
                                    self.df.state.unique()))
        return StateData(state_df, True)

    def get_df(self) -> pd.DataFrame:
        return convert_to_deltas(self.df)


class CovidTrackingData(_NationalData):

    def __init__(self):
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
        self._column_mapping = {
            'date': 'date',
            'positiveIncrease': 'cases',
            'totalTestResultsIncrease': 'tests',
            'deathIncrease': 'deaths'
        }

    def _load_df(self, target):
        if target == 'usa':
            url = f'https://covidtracking.com/api/v1/us/daily.csv'
        else:
            url = f'https://covidtracking.com/api/v1/states/{target}/daily.csv'

        csv_path = _dl_csv(url, 'covidtracking', target)

        # load data frame
        df = pd.read_csv(csv_path, parse_dates=['date'],
                         usecols=self._column_mapping.keys())

        # map columns
        df.rename(columns=self._column_mapping, inplace=True)
        df.sort_values('date', inplace=True)

        return df

    def get_state_data(self, state_str) -> StateData:
        df = self._load_df(state_str)
        return StateData(df, False)

    def get_df(self) -> pd.DataFrame:
        return self._load_df('usa')


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

    def add_agg_columns(self, df, window):
        pass

    def _load_data_frame(self, target, window, start_date, end_date):
        csv_path = self.dl_csv_data(target)

        df = pd.read_csv(csv_path, parse_dates=[self.date_column])
        # map column names
        df.rename(columns=self.column_map, inplace=True)
        # oldest to newest
        # df = df.reindex(index=df.index[::-1])
        df.sort_values('date', inplace=True)
        #
        # Add aggregate columns of interest
        self.add_agg_columns(df, window)

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

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        with open(out_path, 'w') as ofp:
            ofp.write(r.text)
        return out_path

    def add_agg_columns(self, df, window):
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


class NyTimes(DataSource):
    def __init__(self):
        column_map = {
            'date': 'date',
            'state': 'state',
            'county': 'county',
            'cases': 'positiveTestResultsCumulative'
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

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        with open(out_path, 'w') as ofp:
            ofp.write(r.text)
        return out_path

    def add_agg_columns(self, df, window):
        grouped = df.groupby('date')
        # grouped.sum['count']
        pass
