import pymongo
import time
import csv

conexion = pymongo.MongoClient("mongodb://localhost:27017/")
#client = MongoClient("mongodb://192.168.0.131:27017,192.168.0.106:27017,192.168.0.104:27017/replicaSet=rs0")
db = conexion["1_n_referenced"]
A = db["A"]
B = db["B"]

def executeQuery(x, y, n):
    query = [
        {'$limit': n},
        {
            '$lookup': {
                'from': 'B',
                'localField': 'idB',
                'foreignField': '_id',
                'as': 'documentsB'
            }
        },
        {
            '$unwind': '$documentsB'
        },
        {
            '$group': {
                '_id': '$_id',
                **{f'a{i}': {'$first': f'$a{i}'} for i in range(1, x+1)},
                'attributesB': {'$push': {f'b{i}': f'$documentsB.b{i}' for i in range(1, y+1)}}
            }
        },
        {
            '$addFields': {
                'num_documents_attributesB': {'$size': '$attributesB'}
            }
        }
    ]

    startTime = time.time()
    result = list(A.aggregate(query))
    finalTime = time.time()
    queryTime = finalTime - startTime

    acum=0
    for i in range(len(result)):
        document = result[i]
        acum = acum + document["num_documents_attributesB"]
    
    return queryTime, acum

attributes = [5,10,15,20,25,30,35,40,45,50]
start = 1
finish = 10001
step = 1
number_repetitions = 10

with open('queries/1_n_referenced.csv',mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['quantity_A','quantity_B','attributes_A','attributes_B','time','cardinality'])
    for i in range(start, finish, step):
        for a in attributes:
            for b in attributes:
                for _ in range(number_repetitions):
                    queryTime, numDocsB = executeQuery(a,b,i)
                    writer.writerow([i,numDocsB,a,b,queryTime,'2'])                    
