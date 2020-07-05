import os
from datetime import datetime

import pandas as pd
import requests

DATA_DIR = "/tmp/covid-testing"


def _dl_csv_data(target):
    """
    https://covidtracking.com/api
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


VALID_STATES = {
    'ak',
    'al',
    'ar',
    'as',
    'az',
    'ca',
    'co',
    'ct',
    'dc',
    'de',
    'fl',
    'ga',
    'gu',
    'hi',
    'ia',
    'id',
    'il',
    'in',
    'ks',
    'ky',
    'la',
    'ma',
    'md',
    'me',
    'mi',
    'mn',
    'mo',
    'mp',
    'ms',
    'mt',
    'nc',
    'nd',
    'ne',
    'nh',
    'nj',
    'nm',
    'nv',
    'ny',
    'oh',
    'ok',
    'or',
    'pa',
    'pr',
    'ri',
    'sc',
    'sd',
    'tn',
    'tx',
    'ut',
    'va',
    'vi',
    'vt',
    'wa',
    'wi',
    'wv',
    'wy',
}
ROLLING_WINDOW_TEST_SUFFIX = 'DayRollingTestRate'


def _set_test_rates(df, window):
    totals = df[['totalTestResultsIncrease', 'positiveIncrease']].rolling(
        window).sum()
    test_rates = totals['positiveIncrease'] / totals['totalTestResultsIncrease']
    df[str(window) + ROLLING_WINDOW_TEST_SUFFIX] = test_rates


def _load_data_frame(target, windows, start_date, end_date):
    csv_path = _dl_csv_data(target)
    """
    These are the columns of the data frame...

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
    df = pd.read_csv(csv_path, parse_dates=['date'])
    # oldest to newest
    df = df.reindex(index=df.index[::-1])
    # add rolling averages
    for window in windows:
        _set_test_rates(df, window)

    # filter start
    if start_date is not None:
        df = df[df.date >= start_date]
    # filter end
    if end_date is not None:
        df = df[df.date <= end_date]

    return df


def load_state_test_df(state, windows=(), start_date=None, end_date=None):
    if state not in VALID_STATES:
        raise ValueError("Invalid state abbreviation '{}'".format(state))

    return _load_data_frame(state, windows, start_date, end_date)


def load_usa_test_df(windows=(), start_date=None, end_date=None):
    df = _load_data_frame('us', windows, start_date, end_date)
    df['state'] = 'USA'
    return df
