
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext()
ssc = StreamingContext(sc, 5)

### 1) 
#Create a Spark Streaming app that reads Velib data from velib.behmo.com (port 9999)
stream = ssc.socketTextStream("velib.behmo.com", 9999)
stations = stream.map(lambda station: json.loads(station))\
    .map(lambda station:(station['contract_name'] + ' ' + station['name'], station['available_bikes']))\

### 2) 
'''QUESTION : Every 5s: print the empty Velib stations
EXPLICATION : on utilise la fonction filtre afin de pouvoir selectionner toute les 5 secondes, 
les stations de velib du stream qui nont aucun nombres de velos disponibles. '''
emp_station = stations.filter(lambda emp_station: emp_station[1]==0)\
    .pprint() 


### 3) 
''' QUESTION : Every 5s: print the Velib stations that have became empty.
 EXPLICATION : On crée une fonction updateNewEmptyStation() qui va nous permettre de 
 tracker exactement les stations devenues libres via l'utilisation d'une updateStatebyKey. 
 Pour résoudre ce type de probléme on peut utiliser des tables logiques en recensant 
 les états et en définissant une formule suivant les valeurs logiques des paramétres considérés et suivant le cas qui nous intéresse
 i.e, la station devient vide.
'''
def updateNewEmptyStation(newValues, tracker):
    if not newValues: # cas ou les données pour une station ne sont pas updatés.
        tracker = 1
    else:
        if tracker is None: #cas ou le tracker n'existe pas encore.
        	tracker = 1
        elif newValues[0] == 0 and tracker == 1 : # cas ou la station devient vide.
        	tracker = 0
        elif newValues[0] == 0 and tracker == 0: # cas ou la station était vide. L'erreur est de remettre le tracker à 1.
        	tracker = 2
        elif newValues[0] != 0: # cas ou la station n'est pas vide.
        	tracker = 1
        else: # autres cas.
            tracker = 2 
    return tracker

tracker_empty_stations = stations.reduceByKey(lambda x, y: y).updateStateByKey(updateNewEmptyStation)
tracker_empty_stations.filter(lambda new_emp_station: new_emp_station[1] == 0 ).pprint()


### 4)
''' QUESTION : Every 1 min: print the stations that were most active during the last 5 min 
(activity = number of bikes borrowed and returned) get the first available_bikes et toute 
les 5 secondes il change. on fait la somme des valeurs absolues de la diff de bike available entre deux fenetre.

EXPLICATION : Pour cette question là. Nous n'avons pas réussi à trouver la solution. En effet l'on souhaitait définir un 
tracker d'activité qui serai updaté et qui prendrait en compte les deltas i.e. activités des stations entres deux instants 
et un reducebykeyandwindow afin de répondre aux spécificités du probléme. Cependant, nous n'avons pas bien réussi 
à conceptualiser et à spécifier suffisament le probléme. Il y a très peu de documentation en ce qui concerne reducebyKeyandWindow
et en ce qui concerne la gestion d'une variable (i.e les deltas qui illustrent l'activité) qui provient du calcul d'autres variables. 
Notamment lors de la définition de la fonction inverse du reducebyKeyandWindow il aurait fallut stocker ces deltas.

#def activityTracker(newValues, compteur):

most_active_station = stations.reduceByKeyAndWindow(lambda c1, c2: abs(c1-c2), None, 60, 30)\
    .transform(lambda rdd: rdd.sortBy(lambda activity_count: -activity_count[1]))\
    .foreachRDD(lambda rdd: rdd.take(10))
    .myRDD.take(n).foreach(println)
'''

ssc.checkpoint("./checkpoint")
ssc.start()
ssc.awaitTermination()

