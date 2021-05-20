import os, sys, time
import argparse
import json
import logging, coloredlogs
import itertools
import datetime

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


def sample_frame(df, time):
    time = range(ms(time)+1)
    nf = pd.DataFrame(columns=['rating'], index=time)
    # Every once in a while we somehow get an entry that isn't sorted by time.
    # Not sure what's causing that.
    df = df.sort_values('time')
    for index, row in df.iterrows():
        timeindex = ms(row['time'])
        nf.iloc[timeindex,0] = row['rating']
    nf = nf.fillna(method='ffill')
    nf = nf.fillna(0.5)
    return nf



def load_original(f):
    # Load the original actor's ratings given a video filename
    m = re.search('(EA\d+-[NP]\d+)', f)
    name = m.group(0)
    this_dir = os.path.dirname(os.path.realpath(__file__))
    csv_name = os.path.join(this_dir, 'original-ratings', f'{name}.csv')
    original = pd.read_csv(csv_name, header=None)
    original.columns = ['rating', 'time']
    # normalize ratings from likert 1-9 to 0-1
    original['rating'] = (original['rating'] - 1) / 8
    return original, name


def fix_ratings(rating, last_original_time):
    rating.rename(columns={"value":"rating", "browserTime":"time"}, inplace=True)
    # print(f"Last rating time is {rating['time'].iloc[-1]}")
    # print(f"Last player time is {rating['player_time'].iloc[-1]}")
    # Browser time is more reliable than the player callback
    rating = rating.drop(columns=['playerTime'])
    # Browser time is in ms
    rating['time'] = rating['time'] / 1000

    # Add a beginning rating that starts at the midpoint
    # (oops, I should have had the task do this)
    rating.loc[-1] = [0.0, 0.5]
    rating.index = rating.index + 1 # shifting index forward

    # Sometimes not sorted, unclear why
    rating = rating.sort_values('time')

    # Add an end rating that just drags out what they started with
    # if they didn't move the mouse for a while
    last_rating_time = rating['time'].iloc[-1]
    last_time = last_rating_time

    if last_original_time > last_rating_time:
        last_time = last_original_time
        new_row = {'time':last_time, 'rating':rating['rating'].iloc[-1]}
        rating = rating.append(new_row, ignore_index=True)

    return rating, last_time


class Unpacker():
    def __init__(self, path, start_date):
        self.conn = sqlite3.connect(path)
        self.start_date = start_date

    def execute(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def select(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def unpack(self):
        # Map of labjs session data by ppt id and then by session
        sessions = {}
        for row in self.select("select * from labjs where metadata like '%\"payload\":\"full\"%'"):
            rowid = row[0]
            # This is the internal unique id for a given "session" of the task, not the "session" passed in via URL parameter
            labjs_session = row[1]

            # The data is a JSON-encoded array in the last column
            data = json.loads(row[5])
            for thing in data:
                if thing['sender'] == 'Instructions Start':
                    if 'ppt' in thing:
                        ppt = thing['ppt']
                        logging.info(f"PPT {ppt}: Reading row {rowid} for session {labjs_session}")

                if thing['sender'] == 'Video':
                    trial_count = 0
                    if 'trial_count' in thing:
                        trial_count = thing['trial_count']
                    ppt = thing['ppt']
                    if not ppt in sessions:
                        sessions[ppt] = {}

                    ppt_session = str(thing['session']) or "1"
                    if not ppt_session in sessions[ppt]:
                        sessions[ppt][ppt_session] = []

                    timestamp = dateutil.parser.parse(thing['timestamp']).astimezone(CST)

                    logging.debug(f"Got video {trial_count} with {len(thing['response'])} mouse movements for {ppt} session {ppt_session} at {timestamp}")

                    to_save = {
                            'affect': thing['affect'],
                            'trial_count': trial_count,
                            'response': thing['response'],
                            'ppt': thing['ppt'],
                            'ppt_session': ppt_session,
                            'labjs_session': labjs_session,
                            'timestamp': timestamp,
                        }
                    if 'video_filename' in thing:
                        to_save['video_filename'] = thing['video_filename']

                    # Now we skip this if the timestamp is after the given start date
                    # (but we add everything if the start date is not specified)
                    if not self.start_date or timestamp.date() >= self.start_date:
                        sessions[ppt][ppt_session].append(to_save)

        return sessions
        

class Aggregator():
    def __init__(self, data):
        # Here we just want to get aggregated "mean" ratings for the comparer to use later

        self.vids = {}
        self.ppts = {}
        self.short_name = {}
        self.original_videos = {}
        self.original_video_length = {}

        # Cache original file lengths
        def get_original_length(name):
            if name in self.original_video_length:
                return self.original_video_length[name]
            else:
                original, short_name = load_original(vid['video_filename'])

                time = original['time'].iloc[-1]
                self.original_video_length[name] = time
                original_sampled = sample_frame(original, time)
                self.original_videos[name] = original_sampled
                self.short_name[name] = short_name
                return time

        for ppt in data.keys():
            for session, vids in data[ppt].items():
                for vid in vids:
                    filename = vid['video_filename']

                    rating = pd.DataFrame(vid['response'])
                    if len(rating.index) > 0:
                        rating, last_time = fix_ratings(rating, get_original_length(filename))
                        # Resample to second bins
                        rating_sampled = sample_frame(rating, last_time)
                        if not filename in self.vids:
                            self.vids[filename] = []
                        if not ppt in self.ppts:
                            self.ppts[ppt] = {}
                        if not session in self.ppts[ppt]:
                            self.ppts[ppt][session] = []
                        self.vids[filename].append(rating_sampled)
                        self.ppts[ppt][session].append({
                            'filename': filename,
                            'rating': rating,
                            'rating_sampled': rating_sampled,
                            'trial_count': vid['trial_count'],
                            'affect': vid['affect'],
                            'labjs_session': vid['labjs_session'],
                            'timestamp': vid['timestamp'],
                        })

        self.means = {}
        # OK, now we have a hash by video file and can average them together...
        for vid in self.vids.keys():
            ratings_for_video = self.vids[vid]
            all_ratings = pd.concat(ratings_for_video, axis=1)
            means = all_ratings.mean(axis=1)
            df_means = pd.DataFrame(means)
            df_means.columns = ['rating']
            self.means[vid] = df_means


class OriginalRaterPlots():
    def __init__(self, agg, output_dir):
        # Make some plots showing original raters plus aggregated mean participant ratings
        for name in agg.original_videos.keys():
            mean_ratings = agg.means[name]
            original_sampled = agg.original_videos[name]

            ax = plt.gca()
            original_sampled.plot(kind='line',use_index=True,y='rating',ax=ax,label='Original Actor')
            mean_ratings.plot(kind='line',use_index=True,y='rating',ax=ax,label='Mean Ratings')
            ax.get_figure().savefig(os.path.join(output_dir, f'video_mean_plot_{name}.png'))
            plt.clf()

class Comparer():
    def __init__(self, ppt, agg, tsvwriter, output_dir):
        for session, trials in agg.ppts[ppt].items():
            if len(trials) != 6:
                logging.warning(f"Got {len(trials)} for {ppt} in {session}")
            # Write summary stats and plots for each trial this participant did
            for trial in trials:
                trial_count = trial['trial_count']
                affect = trial['affect']
                timestamp = trial['timestamp']
                name = trial['filename']

                # Get original actor's ratings, previously loaded and resampled by aggregator
                original_sampled = agg.original_videos[name]
                short_name = agg.original_videos[name]

                # Get the mean ratings for this video
                mean_ratings = agg.means[name]

                # Compare with our resampled ratings for this trial
                rating_sampled = trial['rating_sampled']

                pearson_correlation_original = original_sampled.corrwith(rating_sampled, axis=0)
                pearson_correlation_mean_participant = mean_ratings.corrwith(rating_sampled, axis=0)
                tsvwriter.writerow([
                    ppt, session, trial_count, affect, timestamp, name,
                    float(pearson_correlation_original),
                    float(pearson_correlation_mean_participant)])

                ax = plt.gca()

                original_sampled.plot(kind='line',use_index=True,y='rating',ax=ax,label='Original Actor')
                mean_ratings.plot(kind='line',use_index=True,y='rating',ax=ax,label='Mean Ratings')
                rating_sampled.plot(kind='line',use_index=True,y='rating',color='red',ax=ax,label=f'Participant {ppt}')

                ax.get_figure().savefig(os.path.join(output_dir, f'plot_{ppt}_{session}_figure{trial_count}.png'))
                plt.clf()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract usable data from lab.js sqlite database for Empathic Accuracy task')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('-s', '--start-date', type=datetime.date.fromisoformat,
            help="Only include data after a given ISO-formatted date")
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
        u = Unpacker(args.db, args.start_date)
        data = u.unpack()

        tsv_path = os.path.join(args.output, f'eat_summary.tsv')
        with open(tsv_path, 'w') as tsvfile:
            tsvwriter = csv.writer(tsvfile, delimiter='\t')
            tsvwriter.writerow(['ppt', 'session', 'trial', 'affect', 'timestamp', 'video_name', 'original_rater_pearson_coefficient', 'mean_participant_pearson_coefficient'])
            agg = Aggregator(data)
            OriginalRaterPlots(agg, args.output)
            for ppt in agg.ppts.keys():
                comp = Comparer(ppt, agg, tsvwriter, args.output)

    else:
        logging.error("DB path does not exist")
        sys.exit(1)
