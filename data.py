import os
from abc import ABC
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

DATA_DIR = "/tmp/covid-testing"

ABV_STATE_MAP = {'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas',
                 'AS': 'American Samoa', 'AZ': 'Arizona', 'CA': 'California',
                 'CO': 'Colorado', 'CT': 'Connecticut',
                 'DC': 'District Of Columbia', 'DE': 'Delaware',
                 'FL': 'Florida', 'GA': 'Georgia', 'GU': 'Guam', 'HI': 'Hawaii',
                 'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana',
                 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
                 'MA': 'Massachusetts', 'MD': 'Maryland', 'ME': 'Maine',
                 'MI': 'Michigan', 'MN': 'Minnesota', 'MO': 'Missouri',
                 'MP': 'Northern Mariana Islands', 'MS': 'Mississippi',
                 'MT': 'Montana', 'NC': 'North Carolina', 'ND': 'North Dakota',
                 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
                 'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York',
                 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon',
                 'PA': 'Pennsylvania', 'PR': 'Puerto Rico',
                 'RI': 'Rhode Island', 'SC': 'South Carolina',
                 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas',
                 'UT': 'Utah', 'VA': 'Virginia', 'VI': 'US Virgin Islands',
                 'VT': 'Vermont', 'WA': 'Washington', 'WI': 'Wisconsin',
                 'WV': 'West Virginia', 'WY': 'Wyoming'}

STATE_ABV_MAP = {v: k for k, v in ABV_STATE_MAP.items()}


def _lookup_name_abbrev(state_str):
    """Return state (name, abbreviation) tuple. Abbrev is UPPERCASE"""
    # Look up by abbreviation
    if state_str.upper() in ABV_STATE_MAP:
        state_str = state_str.upper()
        return ABV_STATE_MAP[state_str], state_str

    if state_str in STATE_ABV_MAP:
        return state_str, STATE_ABV_MAP[state_str]

    raise KeyError("Failed to find state string {}".format(state_str))


class Location(object):
    def __init__(self, nation, state, county):
        self.nation = nation
        self.county = county
        self.state = state


def parse_location(location_string: str) -> Location:
    data = [_.strip() for _ in location_string.split(",")]
    # only have USA data
    nation, state, county = "USA", None, None

    unparsed = []
    for dat in data:
        if dat.upper() == 'USA':
            continue
        elif dat.upper() in ABV_STATE_MAP:
            state = dat
        elif dat in STATE_ABV_MAP:
            state = dat
        else:
            unparsed.append(dat)

    # we should only have one un-parsed param
    if len(unparsed) > 1:
        raise ValueError("Could not parse '{}' un-parsed datum {}"
                         .format(location_string, unparsed))
    if len(unparsed) == len(data):
        raise ValueError("Failed to parse location '{}' -- check casing? ")

    if unparsed:
        county = unparsed[0]

    return Location(nation, state, county)


#
#
#

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
    # this is an awful lot of screwing around to assure that the
    # delta @t0 is correct
    before_time = pd.DataFrame(
        data={
            # 'date': [df['date'].iloc[0]],
            'cases': [0],
            'deaths': [0]
        },
    )
    extended = before_time.append(cumulative)
    deltas = extended.diff().dropna().reset_index(drop=False).rename(
        columns={'index': 'date'})
    return deltas


class DataUnavailableException(Exception):
    def __init__(self, *args):
        super(DataUnavailableException, self).__init__(*args)


POSITIVE_CASE_COL = 'cases'
TEST_TOTAL_COL = 'tests'
DEATHS_COL = 'deaths'

NON_NUMERIC_COLUMNS = {
    'date', 'nation', 'state', 'county', 'location'
}


def date_filter(df: pd.DataFrame,
                start_date: Optional[pd.Timestamp] = None,
                end_date: Optional[pd.Timestamp] = None):
    """Inclusive date filtering"""
    if start_date:
        df = df[df.date >= start_date]
    if end_date:
        df = df[df.date <= end_date]
    return df


def add_avg_columns(df: pd.DataFrame, window: int):
    # First find rolling means
    numeric = df.drop(NON_NUMERIC_COLUMNS, axis=1, errors='ignore')
    roller = numeric.rolling(window)
    means = roller.mean()
    df = df.join(means, rsuffix='_{}day-avg'.format(window))

    if TEST_TOTAL_COL in df.columns:
        totals = roller.sum()
        test_rates = (totals[POSITIVE_CASE_COL] /
                      totals[TEST_TOTAL_COL])
        df['test-rate_{}day-avg'.format(window)] = test_rates

    return df


class DailyData(object):

    def get_df(self) -> pd.DataFrame:
        """Returns a data frame with state (and county) plus other columns"""
        raise NotImplementedError

    def get_avg_df(self, window) -> pd.DataFrame:
        return add_avg_columns(self.get_df(), window)


class _StateData(DailyData, ABC):

    def get_county_data(self, county_str) -> DailyData:
        raise NotImplementedError("County data unavailable")


class _NationalData(DailyData, ABC):

    def get_state_data(self, state_str) -> _StateData:
        raise NotImplementedError("State data not available")

    def build_source(self, loc: Location):
        source = self

        if loc.state:
            source = source.get_state_data(loc.state)

        if loc.county:
            source = source.get_county_data(loc.county)

        return source

    def build_df(self, loc: Location, window: int,
                 start_date=None, end_date=None) -> pd.DataFrame:
        source = self.build_source(loc)
        df = source.get_avg_df(window)
        return date_filter(df, start_date, end_date)


def add_location_info(df: pd.DataFrame, nation: str, state: str, county: str):
    df['nation'] = nation
    df['state'] = state
    df['county'] = county
    location = " ".join([_ for _ in (county, state, nation) if _])
    df['location'] = location


class CountyData(DailyData):
    def __init__(self, df: pd.DataFrame):
        assert len(df[['state', 'county']].drop_duplicates()) == 1
        self.df = df

    def get_df(self) -> pd.DataFrame:
        deltas = convert_to_deltas(self.df)
        add_location_info(deltas, 'USA',
                          self.df['state'].iloc[0],
                          self.df['county'].iloc[0])
        return deltas


class StateData(_StateData):

    def __init__(self, df: pd.DataFrame,
                 is_aggregate: bool):
        # 'state' should always be present
        assert len(df['state'].unique()) == 1
        self.df = df
        self.is_aggregate = is_aggregate

    def get_df(self) -> pd.DataFrame:
        if self.is_aggregate:
            df = convert_to_deltas(self.df)
        else:
            df = self.df

        add_location_info(df, 'USA',
                          self.df['state'].iloc[0],
                          None)
        return df

    def get_county_data(self, county_str) -> DailyData:
        if 'county' not in self.df.columns:
            raise DataUnavailableException("County data not available.")

        county_df = self.df[self.df.county == county_str]
        if county_df.empty:
            raise ValueError("Invalid state {} choose from {}".
                             format(county_df,
                                    self.df.county.unique()))
        return CountyData(county_df)


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
        name, state = _lookup_name_abbrev(state_str)
        state_df = self.df[self.df.state == name]
        if state_df.empty:
            raise ValueError("Invalid state {} choose from {}".
                             format(name,
                                    self.df.state.unique()))
        return StateData(state_df, True)

    def get_df(self) -> pd.DataFrame:
        df = convert_to_deltas(self.df)
        add_location_info(df, 'USA',
                          None, None)
        return df


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
        name, state = _lookup_name_abbrev(state_str)
        df = self._load_df(state.lower())
        df['state'] = name
        return StateData(df, False)

    def get_df(self) -> pd.DataFrame:
        df = self._load_df('usa')
        add_location_info(df, 'USA',
                          None, None)
        return df


class PopulationData(object):
    def build_df(self, loc: Location) -> pd.DataFrame:
        raise NotImplementedError

    def get_population(self, loc: Location) -> int:
        raise NotImplementedError


CENSUS_DIR = "/tmp/us-census"


def _dl_census_csv():
    csv_file = os.path.join(CENSUS_DIR, 'co-est2019-alldata.csv')
    if os.path.exists(csv_file):
        return csv_file

    url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv'
    resp = requests.get(url)
    if resp.status_code != 200:
        raise ValueError("Bad error code\n{}", resp)

    if not os.path.exists(CENSUS_DIR):
        os.makedirs(CENSUS_DIR)

    with open(csv_file, 'w') as ofp:
        ofp.write(resp.text)

    return csv_file


def _fix_county_name(name: str):
    return (name
            .replace(" County", "")
            .replace(" Borough", "")
            .replace(" Parish", "")
            )


def _load_census_df():
    csv_path = _dl_census_csv()
    fields = ['SUMLEV', 'STNAME', 'CTYNAME', 'POPESTIMATE2019']
    df = pd.read_csv(csv_path, usecols=fields)
    # map columns
    col_map = {
        "SUMLEV": 'sumlev',
        "STNAME": 'state',
        "CTYNAME": 'county',
        'POPESTIMATE2019': 'population'
    }
    df.rename(columns=col_map, inplace=True)
    # strip " County" Borough, Census Area, Parish
    df['county'] = df['county'].apply(_fix_county_name)
    # keep only county level
    county = df[df['sumlev'] == 50]

    return county.drop(columns=['sumlev'])


class CensusData(PopulationData):
    def __init__(self):
        self.df = _load_census_df()

    def build_df(self, loc: Location) -> pd.DataFrame:
        if loc.nation != 'USA':
            raise ValueError("Unknown nation: {}".format(loc.nation))

        df = self.df
        if loc.state:
            name, abbrev = _lookup_name_abbrev(loc.state)
            df = df[df.state == name]
            assert not df.empty, "WTF state is '{}'".format(name)

        if loc.county:
            df = df[df.county == loc.county]
            assert len(df) == 1, \
                "Expected only one county name per state?\nGot: {}".format(df)

        return df

    def get_population(self, loc: Location) -> int:
        df = self.build_df(loc)
        return df['population'].sum()


class PopulationNormalizedData(object):

    def __init__(self, covid_data: _NationalData, census_data: PopulationData):
        self.covid_data = covid_data
        self.census_data = census_data

    def build_df(self, loc: Location, window: int,
                 start_date=None, end_date=None) -> pd.DataFrame:
        raw_df = self.covid_data.build_source(loc).get_df()

        population = self.census_data.get_population(loc)
        pop100k = population / 100e3

        for col in ['cases', 'deaths', 'tests']:
            if col in raw_df.columns:
                raw_df['{}100k'.format(col)] = raw_df[col] / pop100k

        avg = add_avg_columns(raw_df, window)
        return date_filter(avg, start_date, end_date)
