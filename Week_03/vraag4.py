# vraag 4: Het aantal woorden in de tekst
from mrjob.job import MRJob

class Vraag4(MRJob):
    def mapper(self, _, line):
        yield ("aantal woorden", len(line.split())) # je kan ook meer dan enkel 1-tjes gaan emitten

    def reducer(self, key, counts):
        yield(key, sum(counts))

if __name__ == '__main__':
    Vraag4.run()
