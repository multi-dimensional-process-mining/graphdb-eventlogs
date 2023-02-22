from tqdm import tqdm
import sys

# HERE BE DRAGONS
# python trickery to make sure that printing of progress bar from tqdm works fine
    # print statements are overwritten as tqdm.write()
# https://stackoverflow.com/questions/36986929/redirect-print-command-in-python-script-through-tqdm-write
class DummyFile(object):
    file = None

    def __init__(self, file):
        self.file = file

    def write(self, x):
        # Avoid print() second call (useless \n)
        if len(x.rstrip()) > 0:
            tqdm.write(x, file=self.file)

# Make class to use __enter__ and __exit__ to avoid using _with_ to open and close context
# https://stackoverflow.com/questions/6796492/temporarily-redirect-stdout-stderr
class Nostdout:
    def __enter__(self):
        self.save_stdout = sys.stdout
        sys.stdout = DummyFile(sys.stdout)

    def __exit__(self, *args, **kwargs):
        sys.stdout = self.save_stdout