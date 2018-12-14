from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")
LAT_RE = re.compile(r"[a-zA-Z]+")
MIN_RE = re.compile(r"[\w\.]\s\w+")


class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in MIN_RE.findall(line.split("\t")[2]):
            a = word[0] == "."
            b = word[2] == word[2].lower()
            if a and b:
                yield word[2:], (1, 0, 0, 0)
            elif a and not b:
                yield word[2:], (0, 1, 0, 0)
            elif not a and b:
                yield word[2:], (0, 0, 1, 0)
            else:
                yield word[2:], (0, 0, 0, 1)

    def combiner(self, word, counts):
        ab = 0
        anb = 0
        nab = 0
        nanb = 0
        for x in counts:
            ab += x[0]
            anb += x[1]
            nab += x[2]
            nanb += x[3]
        yield word, (ab, anb, nab, nanb)

    def reducer(self, word, counts):
        ab = 0
        anb = 0
        nab = 0
        nanb = 0
        for x in counts:
            ab += x[0]
            anb += x[1]
            nab += x[2]
            nanb += x[3]
        if ab + nab == 0 and nanb > 0 and nanb + anb > 10:
            yield word, "%i, %i, %i, %i" % (ab, anb, nab, nanb)


if __name__ == '__main__':
    MRWordFreqCount.run()
