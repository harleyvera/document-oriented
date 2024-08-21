from pymongo import MongoClient
import random
import string
from datetime import datetime

#client = MongoClient("mongodb://192.168.0.131:27017,192.168.0.106:27017,192.168.0.104:27017/replicaSet=rs0")
client = MongoClient("localhost:27017")

db1 = client["1_n_referenced"]

collection_A1 = db1["A"] 
collection_B1 = db1["B"]

def definingArrayComponents():
    dataTypeArray = ["String","Integer","Boolean","Double","Timestamp"]
    dataType = random.choice(dataTypeArray)

    if dataType == "String":
        rdNumber = random.randint(0,50)
        return "".join(random.choices(string.ascii_lowercase, k = rdNumber))
    elif dataType == "Integer":
        return random.randint(0,2147483647)
    elif dataType == "Boolean":
        Booleanos = [True,False]
        return random.choice(Booleanos)
    elif dataType == "Double":
        return random.uniform(0.0, 1000.1)
    elif dataType == "Timestamp":
        return datetime.now()

def definingValues():
    dataTypeArray = ["String","Integer","Boolean","Double","Timestamp", "Array"]
    dataType = random.choice(dataTypeArray)

    if dataType == "String":
        rdNumber = random.randint(0,50)
        return "".join(random.choices(string.ascii_lowercase, k = rdNumber))
    elif dataType == "Integer":
        return random.randint(0,2147483647)
    elif dataType == "Boolean":
        Booleanos = [True,False]
        return random.choice(Booleanos)
    elif dataType == "Double":
        return random.uniform(0.0, 1000.1)
    elif dataType == "Timestamp":
        return datetime.now()
    elif dataType == "Array":
        return[definingArrayComponents() for k in range(1,random.randint(1,50))]

range_intervals = 10000
documents_quantity = 10000000

for k in range(1,documents_quantity,range_intervals):
    for i in range(range_intervals):
        
        toInsertA = []
        toInsertB = []

        B = {
        "_id": k + i,
        "b1": definingValues(),
        "b2": definingValues(),
        "b3": definingValues(),
        "b4": definingValues(),
        "b5": definingValues(),
        "b6": definingValues(),
        "b7": definingValues(),
        "b8": definingValues(),
        "b9": definingValues(),
        "b10": definingValues(),
        "b11": definingValues(),
        "b12": definingValues(),
        "b13": definingValues(),
        "b14": definingValues(),
        "b15": definingValues(),
        "b16": definingValues(),
        "b17": definingValues(),
        "b18": definingValues(),
        "b19": definingValues(),
        "b20": definingValues(),
        "b21": definingValues(),
        "b22": definingValues(),
        "b23": definingValues(),
        "b24": definingValues(),
        "b25": definingValues(),
        "b26": definingValues(),
        "b27": definingValues(),
        "b28": definingValues(),
        "b29": definingValues(),
        "b30": definingValues(),
        "b31": definingValues(),
        "b32": definingValues(),
        "b33": definingValues(),
        "b34": definingValues(),
        "b35": definingValues(),
        "b36": definingValues(),
        "b37": definingValues(),
        "b38": definingValues(),
        "b39": definingValues(),
        "b40": definingValues(),
        "b41": definingValues(),
        "b42": definingValues(),
        "b43": definingValues(),
        "b44": definingValues(),
        "b45": definingValues(),
        "b46": definingValues(),
        "b47": definingValues(),
        "b48": definingValues(),
        "b49": definingValues(),
        "b50": definingValues()}
        toInsertB.append(B)
        A = {
        "_id": k + i,
        "a1": definingValues(),
        "a2": definingValues(),
        "a3": definingValues(),
        "a4": definingValues(),
        "a5": definingValues(),
        "a6": definingValues(),
        "a7": definingValues(),
        "a8": definingValues(),
        "a9": definingValues(),
        "a10": definingValues(),
        "a11": definingValues(),
        "a12": definingValues(),
        "a13": definingValues(),
        "a14": definingValues(),
        "a15": definingValues(),
        "a16": definingValues(),
        "a17": definingValues(),
        "a18": definingValues(),
        "a19": definingValues(),
        "a20": definingValues(),
        "a21": definingValues(),
        "a22": definingValues(),
        "a23": definingValues(),
        "a24": definingValues(),
        "a25": definingValues(),
        "a26": definingValues(),
        "a27": definingValues(),
        "a28": definingValues(),
        "a29": definingValues(),
        "a30": definingValues(),
        "a31": definingValues(),
        "a32": definingValues(),
        "a33": definingValues(),
        "a34": definingValues(),
        "a35": definingValues(),
        "a36": definingValues(),
        "a37": definingValues(),
        "a38": definingValues(),
        "a39": definingValues(),
        "a40": definingValues(),
        "a41": definingValues(),
        "a42": definingValues(),
        "a43": definingValues(),
        "a44": definingValues(),
        "a45": definingValues(),
        "a46": definingValues(),
        "a47": definingValues(),
        "a48": definingValues(),
        "a49": definingValues(),
        "a50": definingValues(),
        "idB": []
        }
        
        N = random.randint(1, 50)
        for j in range(N):
            rmB = random.randint(0, documents_quantity)
            A["idB"].append(rmB)
            
        toInsertA.append(A)
                    
        collection_A1.insert_many(toInsertA)
        collection_B1.insert_many(toInsertB)     
