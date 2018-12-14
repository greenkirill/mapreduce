from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")
LAT_RE = re.compile(r"[a-zA-Z]+")

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in LAT_RE.findall(line.split("\t")[2]):
            yield word, 1

    def combiner(self, word, counts):
        yield "wrd", (word, sum(counts))

    def reducer(self, word, counts):
        counts = list(counts)
        mx = max(counts, key=lambda x:x[1])
        yield str(mx[0]), str(mx[1])


if __name__ == '__main__':    
    MRWordFreqCount.run()

    