import argparse
import datetime
import logging, coloredlogs
import numpy as np
import pandas as pd

def rotate(df):
    # Get means of the two pearson coefficients across trials, and then pivot
    means = df.drop(columns='trial').groupby(['ppt', 'session']).agg('mean')
    return means.unstack()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pivot and average for masterfile use')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('input')
    parser.add_argument('output')
    args = parser.parse_args()

    if args.verbose:
        if args.verbose > 1:
            coloredlogs.install(level='DEBUG')
        elif args.verbose > 0:
            coloredlogs.install(level='INFO')
    else:
        coloredlogs.install(level='WARN')

    df = pd.read_csv(args.input, sep='\t', low_memory=False, parse_dates=['timestamp'])

    rotated = rotate(df)

    rotated.to_csv(args.output, sep='\t')

