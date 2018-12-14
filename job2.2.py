from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line.split("\t")[2]):           
            yield "wrd", len(word)


    def reducer(self, word, counts):
        counts = list(counts)
        yield " ", str(sum(counts)/len(counts))


if __name__ == '__main__':    
    MRWordFreqCount.run()