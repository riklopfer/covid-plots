#!/usr/bin/env python3.7
import argparse
import sys

import pandas as pd
import plotly.express as px

import data


def main(argv):
    parser = argparse.ArgumentParser(description=argv[0])
    parser.add_argument('locations',
                        help='County, state (full). e.g. Clark,OH',
                        type=str,
                        nargs='+'
                        )
    parser.add_argument('--start',
                        help='start date',
                        type=pd.to_datetime,
                        default=None
                        )
    parser.add_argument('--end',
                        help='end date',
                        type=pd.to_datetime,
                        default=None
                        )
    parser.add_argument('--window',
                        help='size of the moving window',
                        type=int,
                        default=7
                        )
    parser.add_argument('--metric',
                        help='Metric to look at.',
                        type=str,
                        choices=['cases', 'deaths', 'test-rate'],
                        default='cases'
                        )
    args = parser.parse_args(argv[1:])
    locations = args.locations
    window = args.window
    metric = args.metric
    start_date = args.start
    end_date = args.end

    data_source = None
    if metric == 'test-rate':
        # we only get test data from here
        data_source = 'tracking'

    def load_df(location_str):
        loc = data.parse_location(location_str)
        assert not data_source or not loc.county, \
            "We do not have test data for counties :("
        return loc.get_df(window, source=data_source,
                          start_date=start_date, end_date=end_date)

    df = None

    for location in locations:
        if df is None:
            df = load_df(location)
        else:
            df = df.append(load_df(location))

    fig = px.line(df,
                  x="date",
                  y='{}_{}day-avg'.format(metric, window),
                  color='location')
    fig.show()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
