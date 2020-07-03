from datetime import datetime
import os

import requests
import pandas as pd

SOURCE_DICT = {
    '': 'https://covidtracking.com/api/v1/states/pa/current.csv'
}

"""
https://covidtracking.com/api/v1/states/pa/current.csv

"""

DATA_DIR = "/tmp/covid-testing"


def dl_csv_data(state):
    out_dir = os.path.join(DATA_DIR, state)
    now_hour = datetime.utcnow().strftime('%Y-%m-%d:%H')
    out_path = os.path.join(out_dir, f'{now_hour}.daily.csv')
    # update once per hour?
    if os.path.exists(out_path):
        return out_path

    url = f'https://covidtracking.com/api/v1/states/{state}/daily.csv'
    r = requests.get(url)
    assert r.status_code == 200

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    with open(out_path, 'w') as ofp:
        ofp.write(r.text)
    return out_path


if __name__ == '__main__':
    csv_path = dl_csv_data('pa')
    df = pd.read_csv(csv_path)
    print(df)
    pass
