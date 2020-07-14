#!/usr/bin/env python3.7
import argparse
import collections
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
                        choices=['cases', 'deaths', 'tests', 'test-rate',
                                 'cases100k', 'deaths100k', 'tests100k'],
                        default='cases100k'
                        )
    parser.add_argument('-o', '--out_file',
                        help='write HTML to this file',
                        type=str,
                        default=None
                        )

    args = parser.parse_args(argv[1:])
    locations = [data.parse_location(_) for _ in args.locations]
    window = args.window
    metric = args.metric
    start_date = args.start
    end_date = args.end
    out_file = args.out_file

    use_tracking = 'test' in metric
    use_nytimes = any(loc.county for loc in locations)

    if use_nytimes and use_tracking:
        raise ValueError("We do not county level test data!")

    covid_data = data.NyTimesData() if use_nytimes else data.CovidTrackingData()
    census_data = data.CensusData()
    pop_normalized = data.PopulationNormalizedData(covid_data, census_data)

    def load_df(loc):
        return pop_normalized.build_df(loc, window=window,
                                       start_date=start_date, end_date=end_date)

    df = None

    for location in locations:
        if df is None:
            df = load_df(location)
        else:
            df = df.append(load_df(location))

    plot_value = '{}_{}day-avg'.format(metric, window)
    hover_set = set(df.columns) - data.NON_NUMERIC_COLUMNS - {plot_value}

    hover_data = collections.OrderedDict()
    for name in sorted(hover_set):
        if name in {'cases', 'deaths'}:
            hover_data[name] = ':'
        else:
            hover_data[name] = ':.3f'

    fig = px.line(df,
                  x="date",
                  y=plot_value,
                  color='location',
                  hover_name='location',
                  hover_data=hover_data
                  )
    if out_file:
        print("Saving HTML to {}".format(out_file))
        fig.write_html(out_file)

    fig.show()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
