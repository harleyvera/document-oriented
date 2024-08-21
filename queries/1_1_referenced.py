from pymongo import MongoClient
import time
import csv

client = MongoClient('mongodb://localhost:27017/')
#client = MongoClient("mongodb://192.168.0.131:27017,192.168.0.106:27017,192.168.0.104:27017/replicaSet=rs0")
db = client["1_1_referenced"]
A = db["A"]
B = db["B"]

def executeQuery(na, nb, n):
    projection_a = {f'a{i}': f'$a{i}' for i in range(1, na + 1)}
    projection_b = {f'b{i}': f'$documentsB.b{i}' for i in range(1, nb + 1)}

    query = [
        {'$limit': n},
        {
            '$lookup': {
                'from': 'B',
                'localField': 'id_B',
                'foreignField': '_id',
                'as': 'documentsB'
            }
        },
        {
            '$project': {
                '_id': 1,
                **projection_a,
                'documentsB': {
                    '_id': '$documentsB._id',
                    **projection_b
                }
            }
        }
    ]

    startTime = time.time()
    A.aggregate(query)
    finalTime = time.time()
    queryTime = finalTime - startTime
    
    return queryTime

attributes = [5,10,15,20,25,30,35,40,45,50]
start = 1
finish = 10001
step = 1
number_repetitions = 10

with open('queries/1_1_referenced.csv',mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['quantity_A','quantity_B','attributes_A','attributes_B','time','cardinality'])
    for i in range(start, finish, step):
        for a in attributes:
            for b in attributes:
                for _ in range(number_repetitions):
                    queryTime = executeQuery(a,b,i)
                    writer.writerow([i,i,a,b,queryTime,'1'])                    
