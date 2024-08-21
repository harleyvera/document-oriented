from pymongo import MongoClient
import time
import csv

client = MongoClient('mongodb://localhost:27017/')
#client = MongoClient("mongodb://192.168.0.131:27017,192.168.0.106:27017,192.168.0.104:27017/replicaSet=rs0")
db = client['1_1_embedded']
coleccion_a = db['A']

def executeQuery(x, y, n):
    query = [
        {'$limit': n},
        #{'$sample': {'size': n}},
        {'$project': {
            '_id': 1,  # Incluir el _id de A
            **{f'a{i}': f'$a{i}' for i in range(1, x+1)},
            'B': {
                '_id': '$B._id',
                **{f'b{i}': f'$B.b{i}' for i in range(1, y+1)}
            }
        }}
    ]

    initialTime = time.time()
    coleccion_a.aggregate(query)
    finalTime = time.time()
    queryTime = finalTime - initialTime

    return queryTime

attributes = [5,10,15,20,25,30,35,40,45,50]
start = 1
finish = 10001
step = 1
number_repetitions = 10

with open('queries/1_1_embedded.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['quantity_A','quantity_B','attributes_A','attributes_B','time','cardinality'])
    for i in range(start,finish,step):
        for a in attributes:
            for b in attributes:
                for _ in range(number_repetitions):
                    timeUsed = executeQuery(a,b,i)
                    writer.writerow([i,i,a,b,timeUsed,'3'])                   
