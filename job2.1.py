from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁ]+")

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line.split("\t")[2]):           
            yield "wrd", word.lower()

    def combiner(self, word, counts):
        mx = max(list(counts), key=lambda x:len(x))
        yield "wrd", mx

    def reducer(self, word, counts):
        mx = max(list(counts), key=lambda x:len(x))
        yield str(mx), str(len(mx))


if __name__ == '__main__':    
    MRWordFreqCount.run()