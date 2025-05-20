from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime
# tip: werk met scripts die in bestanden gaan geplaatst worden indien je werkt met streaming
# maakt het makkelijker om streams te stoppen
# deze bestanden moeten standalone zijn: maak de sparkcontext opnieuw aan + zorg voor de nodige imports

# Create a local StreamingContext with two working thread and batch interval of 5 second
sc = SparkContext("local[2]", "networkwordcount")
sc.setLogLevel("ERROR") # reduce spam of logging
ssc = StreamingContext(sc, 5) # om de 5 seconden

# ALS JE WERKT MET STATE MOET JE DIT TOEVOEGEN
ssc.checkpoint('checkpoint') # argument is een pad waar de checkpoints bewaard worden

lines = ssc.socketTextStream('localhost', 19999) # de poort waarop deze applicatie luistert naar binnenkomende berichten
words = lines.flatMap(lambda line: line.split())
pairs = words.map(lambda x: (x,datetime.now())) # in het geval we werken met de timestampss
counts = pairs.reduceByKey(lambda x,y : x+y)

# hou een state bij
def updateFunction(new_values, running_count):
    # concept lijkt om het maken van een accumulator
    return sum(new_values) + (running_count or 0) # nieuwe waarde + oude waarde (of 0 als het de eerste is)

# overschrijf de vorige functie
def updateFunction(new_values, timestamp):
    # return de laatste keer dat een woord is gezien geweest
    return datetime.now()

# overschrijf de vorige functie
def updateFunction(new_values, timestamps):
    # return de laatste keer dat een woord is gezien geweest
    if timestamps is None:
        timestamps = []
    timestamps.extend(new_values)
    return timestamps
    
runningCounts = counts.updateStateByKey(updateFunction)

runningCounts.pprint()

ssc.start()
ssc.awaitTermination() # zorg ervoor dat de applicatie actief blijft
