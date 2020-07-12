import unittest

import pandas as pd

import data

_required_columns = {
    'date', 'state'
}


class GenericTest(unittest.TestCase):

    def assert_has_required_columns(self, df):
        missing = _required_columns - set(df.columns)
        self.assertFalse(missing, "Some columns missing {}".format(missing))

    def assert_date_ascending(self, df):
        date_diff = df.date.diff().iloc[1:]
        day_diff = date_diff == pd.Timedelta(1, unit='D')
        # multi day diff
        # day_diff = date_diff >= pd.Timedelta(1, unit='D')
        self.assertTrue(day_diff.all())

    def basic_requirements(self, df):
        self.assertFalse(df.empty, "Data frame is empty")
        self.assert_has_required_columns(df)
        self.assert_date_ascending(df)

    def validate_date_filter(self, df, start_date=None, end_date=None):
        ranged = data.date_filter(df, start_date, end_date)
        if start_date:
            self.assertTrue((ranged.date >= start_date).all())
        if end_date:
            self.assertTrue((ranged.date <= end_date).all())


class NyTimesDataTest(GenericTest):
    def setUp(self) -> None:
        self.data = data.NyTimesData()

    def test_usa_data(self):
        national = self.data.get_df()
        self.basic_requirements(national)

        with_average = self.data.get_avg_df(7)
        self.basic_requirements(with_average)

    def test_pa_data(self):
        pa = self.data.get_state_data('Pennsylvania')
        pa_data = pa.get_df()
        self.basic_requirements(pa_data)

        pa = self.data.get_state_data('PA')
        pa_data = pa.get_df()
        self.basic_requirements(pa_data)

    def test_allegheny(self):
        allegheny = (self.data
                     .get_state_data('Pennsylvania')
                     .get_county_data('Allegheny'))
        adf = allegheny.get_df()
        self.basic_requirements(adf)

        avg = allegheny.get_avg_df(7)
        self.basic_requirements(avg)

    def test_date_range(self):
        ca = (self.data
              .get_state_data('CA')
              .get_county_data("Contra Costa"))
        avg = ca.get_avg_df(7)
        self.validate_date_filter(avg,
                                  start_date=pd.to_datetime('03-22-2020'),
                                  end_date=pd.to_datetime('06-30-2020'))


class CovidTrackingDataTest(GenericTest):
    def setUp(self) -> None:
        self.data = data.CovidTrackingData()

    def test_usa_data(self):
        national = self.data.get_df()
        self.basic_requirements(national)

        with_average = self.data.get_avg_df(7)
        self.basic_requirements(with_average)

    def test_pa_data(self):
        pa = self.data.get_state_data('PA')
        pa_data = pa.get_df()
        self.basic_requirements(pa_data)

        avg = pa.get_avg_df(7)
        self.basic_requirements(avg)

    def test_allegheny(self):
        with self.assertRaises(data.DataUnavailableException):
            allegheny = (self.data
                         .get_state_data('PA')
                         .get_county_data('Allegheny'))

    def test_date_range(self):
        ca = (self.data
              .get_state_data('CA'))
        avg = ca.get_avg_df(7)
        self.validate_date_filter(avg,
                                  start_date=pd.to_datetime('03-22-2020'),
                                  end_date=pd.to_datetime('06-30-2020'))


if __name__ == '__main__':
    unittest.main()
