from mrjob.job import MRJob #importeer mrjob package

class MRWordCount(MRJob): # maak een mapreduce applicatie aan
    def mapper(self, _, line):
        # wordt lijn per lijn uitgevoerd
        for word in line.split():
            # itereer over de woorden in elke lijn
            yield (word, 1) # dit is de emit -> yield is zoals een return maar hij gaat blijven verder doen terwijl een return de functie stopt

    # in de achtergrond gebeurt er dan een shuffle stap -> alle values van dezelfde key worden bij elkaar gevoegd.
    # elke key met zijn lijstje waarden wordt naar de reducer gestuurd
    
    def reducer(self, word, counts):
        # deze wordt per key opgeroepen
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordCount.run() # start de applicatie
