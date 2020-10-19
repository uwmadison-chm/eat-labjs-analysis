import os, sys, time
import argparse
import json
import logging, coloredlogs
import itertools

# unpacker
import sqlite3
import dateutil.parser
import pytz

# comparer
import re
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import csv


def ms(seconds):
    return math.floor(seconds * 1000)


CST = pytz.timezone('US/Central')


class Unpacker():
    def __init__(self, path):
        self.conn = sqlite3.connect(path)

    def execute(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def select(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def unpack(self):
        # Map of labjs session data by ppt id
        sessions = {}
        for row in self.select("select * from labjs where metadata like '%\"payload\":\"full\"%'"):
            rowid = row[0]
            session = row[1]

            # The data is a JSON-encoded array in the last column
            data = json.loads(row[5])
            for thing in data:
                if thing['sender'] == 'Instructions Start':
                    if 'ppt' in thing:
                        ppt = thing['ppt']
                        logging.info(f"PPT {ppt}: Reading row {rowid} for session {session}")

                if thing['sender'] == 'Video':
                    trial_count = 0
                    if 'trial_count' in thing:
                        trial_count = thing['trial_count']
                    ppt = thing['ppt']
                    if not ppt in sessions:
                        sessions[ppt] = []

                    timestamp = dateutil.parser.parse(thing['timestamp']).astimezone(CST)
                    logging.debug(f"Got video {trial_count} with {len(thing['response'])} mouse movements for {ppt} at {timestamp}")

                    to_save = {
                            'affect': thing['affect'],
                            'trial_count': trial_count,
                            'response': thing['response'],
                            'ppt': thing['ppt'],
                            'timestamp': timestamp,
                        }
                    if 'video_filename' in thing:
                        to_save['video_filename'] = thing['video_filename']
                    sessions[ppt].append(to_save)

        return sessions
        

class Comparer():
    def __init__(self, tsvwriter, ppt, data, output_dir):
        self.data = data
        if not data:
            return

        for vid in data:
            trial = vid['trial_count']
            affect = vid['affect']
            timestamp = vid['timestamp']

            # Get the original actor's ratings
            f = vid['video_filename']
            m = re.search('(EA\d+-[NP]\d+)', f)
            name = m.group(0)
            this_dir = os.path.dirname(os.path.realpath(__file__))
            csv_name = os.path.join(this_dir, 'original-ratings', f'{name}.csv')
            original = pd.read_csv(csv_name, header=None)
            original.columns = ['rating', 'time']
            # normalize ratings from likert 1-9 to 0-1
            original['rating'] = (original['rating'] - 1) / 8

            # Get this trial's ratings, storing the raw mouse movements
            rating = pd.DataFrame(vid['response'])
            rating_file = os.path.join(output_dir, f"raw_{ppt}_{trial}.tsv")
            rating.to_csv(rating_file, sep='\t', index=False)
    
            # Plot n' compare them!
            if len(rating.index) > 0:
                rating.columns = ['time', 'player_time', 'rating']
                # Not using the player callback time, browser time should be closer to accurate
                rating = rating.drop(columns=['player_time'])
                rating['time'] = rating['time'] / 1000

                # Add a beginning rating that starts at the midpoint
                # (oops, I should have had the task do this)
                rating.loc[-1] = [0.0, 0.5]
                rating.index = rating.index + 1 # shifting index forward
                rating.sort_index(inplace=True) 

                # Add an end rating that just drags out what they started with
                # if they didn't move the mouse for a while
                last_rating_time = rating['time'].iloc[-1]
                last_original_time = original['time'].iloc[-1]
                last_time = last_rating_time

                if last_original_time > last_rating_time:
                    last_time = last_original_time
                    new_row = {'time':last_time, 'rating':rating['rating'].iloc[-1]}
                    rating = rating.append(new_row, ignore_index=True)

                # BUT FIRST, let's make sampled dataframes that have data for every millisecond
                original_sampled = self.sample_frame(original, last_time)
                rating_sampled = self.sample_frame(rating, last_time)

                pearsonCorrelation = original_sampled.corrwith(rating_sampled, axis=0)
                tsvwriter.writerow([ppt, trial, affect, timestamp, name, float(pearsonCorrelation)])

                ax = plt.gca()

                original_sampled.plot(kind='line',use_index=True,y='rating',ax=ax,label='Original Actor')
                rating_sampled.plot(kind='line',use_index=True,y='rating',color='red',ax=ax,label=f'Participant {ppt}')

                ax.get_figure().savefig(os.path.join(output_dir, f'plot_{ppt}_figure{trial}.png'))
                plt.clf()

    def sample_frame(self, df, time):
        time = range(ms(time)+1)
        nf = pd.DataFrame(columns=['rating'], index=time)
        for index, row in df.iterrows():
            timeindex = ms(row['time'])
            nf.iloc[timeindex,0] = row['rating']
        nf = nf.fillna(method='ffill')
        return nf

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract usable data from lab.js sqlite database for Empathic Accuracy task')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('db')
    parser.add_argument('output')
    args = parser.parse_args()

    if args.verbose:
        if args.verbose > 1:
            coloredlogs.install(level='DEBUG')
        elif args.verbose > 0:
            coloredlogs.install(level='INFO')
    else:
        coloredlogs.install(level='WARN')

    if os.path.exists(args.db):
        u = Unpacker(args.db)
        data = u.unpack()

        tsv_path = os.path.join(args.output, f'eat_summary.tsv')
        with open(tsv_path, 'w') as tsvfile:
            tsvwriter = csv.writer(tsvfile, delimiter='\t')
            tsvwriter.writerow(['ppt', 'trial', 'affect', 'timestamp', 'video_name', 'original_rater_pearson_coefficient'])
            for ppt in data.keys():
                comp = Comparer(tsvwriter, ppt, data[ppt], args.output)

    else:
        logging.error("DB path does not exist")
        sys.exit(1)
