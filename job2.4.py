from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")
LAT_RE = re.compile(r"[a-zA-Z]+")


class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line.split("\t")[2]):
            if word[0] == word[0].lower():
                yield word, (1, 0)
            else:
                yield word, (0, 1)

    def combiner(self, word, counts):
        l = 0
        u = 0
        for x in counts:
            l += x[0]
            u += x[1]
        yield word, (l, u)

    def reducer(self, word, counts):
        l = 0
        u = 0
        for x in counts:
            l += x[0]
            u += x[1]
        if l+u > 10 and l < u:
            yield word, "%i, %i" % (l, u)


if __name__ == '__main__':
    MRWordFreqCount.run()
