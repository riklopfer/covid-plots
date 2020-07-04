import os
from datetime import datetime

import pandas as pd
import requests

DATA_DIR = "/tmp/covid-testing"


def dl_state_csv_data(state):
    """
    https://covidtracking.com/api
    """
    state = state.lower()
    # this doesn't have county-level testing data
    out_dir = os.path.join(DATA_DIR, 'covidtracking', state)
    now_hour = datetime.utcnow().strftime('%Y-%m-%d:%H')
    out_path = os.path.join(out_dir, f'{now_hour}.daily.csv')
    # update once per hour?
    if os.path.exists(out_path):
        return out_path

    url = f'https://covidtracking.com/api/v1/states/{state}/daily.csv'
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


ROLLING_WINDOW_TEST_SUFFIX = 'DayRollingTestRate'


def _set_test_rates(df, window):
    totals = df[['totalTestResultsIncrease', 'positiveIncrease']].rolling(
        window).sum()
    test_rates = totals['positiveIncrease'] / totals['totalTestResultsIncrease']
    df[str(window) + ROLLING_WINDOW_TEST_SUFFIX] = test_rates


def load_test_df(state, windows):
    csv_path = dl_state_csv_data(state)
    """
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
    return df
