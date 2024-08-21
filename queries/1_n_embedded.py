from pymongo import MongoClient
import time
import csv

client = MongoClient('mongodb://localhost:27017/')
#client = MongoClient("mongodb://192.168.0.131:27017,192.168.0.106:27017,192.168.0.104:27017/replicaSet=rs0")
db = client['1_n_embedded']
coleccion_a = db['A']

def executeQuery(x, y, n):
    query = [
        {'$limit': n},
        {'$project': {
            **{f'a{i}': f'$a{i}' for i in range(1, x+1)},
            'attributesB': {f'b{i}': f'$B_docs.b{i}' for i in range(1, y+1)},
            'numDocumentsB': {'$size': '$B_docs'}
        }}
    ]

    startTime = time.time()
    result = list(coleccion_a.aggregate(query))
    finalTime = time.time()
    queryTime = finalTime - startTime

    acum = 0
    for i in range(len(result)):
        document = result[i]
        acum = acum + document['numDocumentsB']

    return queryTime, acum

attributes = [5,10,15,20,25,30,35,40,45,50]
start = 1
finish = 10001
step = 1
number_repetitions = 10

with open('queries/1_n_embedded.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['quantity_A','quantity_B','attributes_A','attributes_B','time','cardinality'])
    for i in range(start,finish,step):
        for a in attributes:
            for b in attributes:
                for _ in range(number_repetitions):
                    queryTime, numDocsB = executeQuery(a,b,i)
                    writer.writerow([i,numDocsB,a,b,queryTime,'4'])
