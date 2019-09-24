import os, sys, time
import argparse
import json
import logging, coloredlogs
import itertools
import sqlite3

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
            #logging.debug(f"Data for {row[0:-2]}")
            for thing in data:
                if thing['sender'] == 'Instructions Start':
                    if 'ppt' in thing:
                        ppt = thing['ppt']
                        logging.info(f"PPT {ppt}: Reading row {rowid} for session {session}")

                if thing['sender'] == 'Video':
                    count = 0
                    if 'trial_count' in thing:
                        count = thing['trial_count']
                    ppt = thing['ppt']
                    if not ppt in sessions:
                        sessions[ppt] = {}
                    logging.debug(f"Got video {count} with {len(thing['response'])} mouse movements for {ppt}")
                    if not str(count) in sessions[ppt]:
                        to_save = {
                                'affect': thing['affect'],
                                'response': thing['response'],
                                'ppt': thing['ppt'],
                            }
                        if 'video_filename' in thing:
                            to_save['video_filename'] = thing['video_filename']
                        sessions[ppt][str(count)] = to_save

        return sessions
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract usable data from lab.js sqlite database for Empathic Accuracy task')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('db')
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

    else:
        logging.error("DB path does not exist")
        sys.exit(1)
