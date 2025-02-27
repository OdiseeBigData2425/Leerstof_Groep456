from mrjob.job import MRJob
import csv
from io import StringIO

col_survived = 1
col_gender = 4
col_age = 5

class MRStructured(MRJob):
    def mapper(self, _, line):

        if line == 'PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked':
            return # verwerk de headerrij niet
        
        csv_file = StringIO(line) # maak een csv_file van de string
        cols = next(csv.reader(csv_file)) # zet de csv_file om naar kolommen -> vermijd problemen met parsen (komma's in namen)

        if cols[col_age] != '':
            yield('leeftijd', float(cols[col_age]))

        if cols[col_survived] != '':
            yield('overleefd', float(cols[col_survived]))
            
        if cols[col_gender] != '':
            yield('mannelijke passagiers', float(cols[col_gender] == 'male'))

        if cols[col_gender] == 'female' and cols[col_survived] != '':
            yield('vrouw overleefd', float(cols[col_survived]))
                
                
        

    def reducer(self, key, counts):
        if key == 'leeftijd':
            # bereken gemiddelde
            counts = list(counts)
            aantal = len(counts)
            totaal = sum(counts)
            yield(key, totaal/aantal)
            yield(key + "_aantal", aantal) # aantal passagiers waar de leeftijd ingevuld was
        elif key == 'overleefd' or key == 'mannelijke passagiers' or key == 'vrouw overleefd':
            # bereken gemiddelde en doe maal 100
            counts = list(counts)
            aantal = len(counts)
            totaal = sum(counts)
            yield(key, f"{totaal/aantal * 100} %")

if __name__ == '__main__':
    MRStructured.run()
