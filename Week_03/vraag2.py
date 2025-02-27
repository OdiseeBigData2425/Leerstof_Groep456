# vraag 2: het aantal keer dat elk karakter voorkomt
from mrjob.job import MRJob

class Vraag2(MRJob):
    def mapper(self, _, line):
        for word in line:
            #yield (word, 1) # emit zelfde key voor alle woorden want de gemiddelde is over alle woorden
            # stel bijvoorbeeld
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield(word, sum(counts))

if __name__ == '__main__':
    Vraag2.run()
