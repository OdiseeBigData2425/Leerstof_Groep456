# vraag 3: * Het aantal woorden dat begint met elke letter
from mrjob.job import MRJob

class Vraag3(MRJob):
    # de _ is eigenlijk dat het niet belangrijk is (je filename, object) -> wordt niet gebruikt
    # de line = is 1 lijn van je bestand
    def mapper(self, _, line):
        for word in line.split():
            yield (word[0], 1) # emit zelfde key voor alle woorden want de gemiddelde is over alle woorden
    # in je mapper emit je een aantal key-value paren bvb [(word1, 1), (word2, 1), (word1, 1)]

    # je reducer wordt met bovenstaande voorbeeld twee keer opgeroepen
    # eerste keer met key = word1 en counts = [1, 1]
    # tweede keer met key = word2 en counts = [1]
    def reducer(self, key, counts):
        # counts is geen list maar een generator (iterator) -> maak er een list van om problemen te vermijden met len of meerdere keren te gebruiken
        yield(key, sum(counts))
    # in je redicer emit je een aantal key-value paren bvb [(word1, 2), (word2, 1)]
    # je output is dan 
    # word1 2
    # word2 1

if __name__ == '__main__':
    Vraag3.run()
