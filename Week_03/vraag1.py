# vraag 1: gemiddelde woordlengte
from mrjob.job import MRJob

class Vraag1(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield ("lengte", len(word)) # emit zelfde key voor alle woorden want de gemiddelde is over alle woorden

    def reducer(self, word, counts):
        # counts is geen list maar een generator (iterator) -> maak er een list van om problemen te vermijden met len of meerdere keren te gebruiken
        counts = list(counts)
        aantal = len(counts)
        totaal = sum(counts)
        yield(word, totaal/aantal)

if __name__ == '__main__':
    Vraag1.run()
