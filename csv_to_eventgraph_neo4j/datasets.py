from enum import Enum

from BPIC14 import BPIC14_sample, BPIC14_full
from BPIC15 import BPIC15_sample, BPIC15_full
from BPIC16 import BPIC16_sample, BPIC16_full
from BPIC17 import BPIC17_sample, BPIC17_full
from BPIC19 import BPIC19_sample, BPIC19_full

class BPICNames(Enum):
    BPIC14_SAMPLE = 1
    BPIC14_FULL = 2
    BPIC15_SAMPLE = 3
    BPIC15_FULL = 4
    BPIC16_SAMPLE = 5
    BPIC16_FULL = 6
    BPIC17_SAMPLE = 7
    BPIC17_FULL = 8
    BPIC19_SAMPLE = 9
    BPIC19_FULL = 10


datasets = {
    BPICNames.BPIC14_SAMPLE: BPIC14_sample,
    BPICNames.BPIC14_FULL: BPIC14_full,
    BPICNames.BPIC15_SAMPLE: BPIC15_sample,
    BPICNames.BPIC15_FULL: BPIC15_full,
    BPICNames.BPIC16_SAMPLE: BPIC16_sample,
    BPICNames.BPIC16_FULL: BPIC16_full,
    BPICNames.BPIC17_SAMPLE: BPIC17_sample,
    BPICNames.BPIC17_FULL: BPIC17_full,
    BPICNames.BPIC19_SAMPLE: BPIC19_sample,
    BPICNames.BPIC19_FULL: BPIC19_full,
}