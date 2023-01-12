import sys
import time
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from context_manager_tqdm import Nostdout


class Performance:
    def __init__(self, perf_file_path: str, perf_file_name:str, number_of_steps:int):
        self.start = time.time()
        self.last = self.start
        self.perf = pd.DataFrame(columns=["name", "start", "end", "duration"])
        self.path = perf_file_path + perf_file_name
        self.count = 0
        self.pbar = tqdm(total=number_of_steps, file=sys.stdout)
        self.total = None
        # start python trickery
        self.ctx = Nostdout()
        self.ctx.__enter__()

    def string_time(self, epoch_time):
        return datetime.utcfromtimestamp(epoch_time).strftime("%H:%M:%S")

    def finished_step(self, activity, log_message):
        end = time.time()
        self.perf = pd.concat([self.perf, pd.DataFrame.from_records([
            {"name": activity,
             "start": self.string_time(self.last),
             "end": self.string_time(end),
             "duration": (end - self.last)}])])
        self.pbar.set_description(f"{log_message}: took {round(end - self.last, 2)} seconds")
        self.last = end
        self.count += 1
        self.pbar.update(1)

    def finish(self):
        end = time.time()
        print(f"{self.count} steps")
        self.perf = pd.concat([self.perf, pd.DataFrame.from_records([
            {"name": "total",
             "start": self.string_time(self.start),
             "end": self.string_time(end),
             "duration": (end - self.start)}])])
        self.total = round(end - self.start, 2)
        print(f"Total: took {round(end - self.start, 2)} seconds")
        self.pbar.set_description(f"Completed")
        self.pbar.close()
        # close python trickery
        self.ctx.__exit__()

    def save(self):
        self.perf.to_csv(self.path)
