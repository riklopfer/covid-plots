#!/usr/bin/env python3.7
import argparse
import logging
import sys
from datetime import datetime
from typing import Iterable

import pandas as pd
import plotly.express as px

import data


def update_locations(locations: Iterable[data.Location], metric: str) -> Iterable[data.Location]:
    use_tracking = 'test' in metric or 'hospitalization' in metric
    if use_tracking:
        # exclude any county-level locations
        return [location.drop_county() for location in locations]
    else:
        return locations


def load_pn_data(metric: str) -> data.PopulationNormalizedData:
    use_tracking = 'test' in metric or 'hospitalization' in metric

    covid_data = data.CovidTrackingData() if use_tracking else data.NyTimesData()
    census_data = data.CensusData()
    pop_normalized = data.PopulationNormalizedData(covid_data, census_data)
    return pop_normalized


def _make_figure(locations: Iterable[data.Location], metric: str, window: int, start_date=None, end_date=None):
    use_tracking = 'test' in metric or 'hospitalization' in metric
    if use_tracking:
        # exclude any county-level locations
        locations = set(location.drop_county() for location in locations)

    if not locations:
        raise data.DataUnavailableException("No data for counties")

    covid_data = data.CovidTrackingData() if use_tracking else data.NyTimesData()
    census_data = data.CensusData()
    pop_normalized = data.PopulationNormalizedData(covid_data, census_data)

    # check if the data has changed

    def load_df(loc):
        return pop_normalized.build_df(loc, window=window,
                                       start_date=start_date, end_date=end_date)

    df = None

    for location in locations:
        if df is None:
            df = load_df(location)
        else:
            df = df.append(load_df(location))

    if window < 2:
        plot_value = metric
    else:
        plot_value = '{}_{}day-avg'.format(metric, window)

    fig = px.line(df,
                  x="date",
                  y=plot_value,
                  color='location',
                  hover_name='location',
                  title='{}'.format(plot_value)
                  )

    return fig


def make_figure(pop_normalized: data.PopulationNormalizedData,
                locations: Iterable[data.Location],
                metric: str, window: int, start_date=None, end_date=None):
    # check if the data has changed

    def load_df(loc):
        return pop_normalized.build_df(loc, window=window,
                                       start_date=start_date, end_date=end_date)

    df = None

    for location in locations:
        if df is None:
            df = load_df(location)
        else:
            df = df.append(load_df(location))

    if window < 2:
        plot_value = metric
    else:
        plot_value = '{}_{}day-avg'.format(metric, window)

    fig = px.line(df,
                  x="date",
                  y=plot_value,
                  color='location',
                  hover_name='location',
                  title='{}'.format(plot_value)
                  )

    return fig


ALLOWED_METRICS = {
    'cases', 'deaths', 'tests', 'hospitalizations',
    'cases100k', 'deaths100k', 'tests100k', 'hospitalizations100k',
    'positive-test-rate',
}


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
    parser.add_argument('--windows',
                        help='size of the moving window. (comma separated)',
                        type=str,
                        required=True
                        )
    parser.add_argument('--metrics',
                        help='Metric to look at. (comma separated)\n'
                             'Allowed: {}'.format(ALLOWED_METRICS),
                        type=str,
                        required=True
                        )
    parser.add_argument('-o', '--out_file',
                        help='write HTML to this file',
                        type=str,
                        default=None
                        )

    args = parser.parse_args(argv[1:])

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(argv[0])

    locations = set(data.parse_location(_) for _ in args.locations)
    windows = [int(_.strip()) for _ in args.windows.split(",") if _.strip()]
    if not windows:
        raise ValueError("Must supply at least one window")
    metrics = []
    for metric in args.metrics.split(","):
        metric = metric.strip()
        if not metric:
            continue
        if metric not in ALLOWED_METRICS:
            raise ValueError("Unknown metric {}\n"
                             "Allowed: {}".format(metric,
                                                  ALLOWED_METRICS))
        metrics.append(metric)
    if not metrics:
        raise ValueError("Must supply at least one metric")
    start_date = args.start
    end_date = args.end
    out_file = args.out_file

    header = ' | '.join(
        f'<a href="#{metric}">{metric}</a>'
        for metric in metrics)
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    html = f'<font size=24>{now_str}</br>{header}</font>'
    for metric in metrics:
        html += '<h2 id={}>{}</h2>'.format(metric, metric)
        for window in windows:
            try:
                updated_locs = update_locations(locations, metric)
                pn_data = load_pn_data(metric)
                fig = make_figure(pop_normalized=pn_data,
                                  locations=updated_locs,
                                  metric=metric,
                                  window=window,
                                  start_date=start_date,
                                  end_date=end_date)
            except data.DataUnavailableException:
                logger.exception("Could not make figure. ")
                continue

            if out_file:
                html += fig.to_html(full_html=False)
            else:
                fig.show()

    if out_file:
        logger.info("Saving HTML to {}".format(out_file))
        with open(out_file, 'w') as ofp:
            ofp.write("<html>{}</html>".format(html))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
