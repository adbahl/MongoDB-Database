#**********************************************
# mongo1.py, using pymongo MongoDB database is created and manipulated
# Also data is read from csv file and updated in database.
#Created By Arvind Bahl
#**********************************************
#importing library
from pymongo import MongoClient
import pandas as pd
import os


# creating a database
client = MongoClient('localhost:27017')
print(client)
print(client.list_database_names())
db = client.test_database
print(db)

#Adding collection to database
courses = db.courses
print(courses)

print(client.list_database_names())

dblist = client.list_database_names()
if "courses" in dblist:
    print("database does exist")

#Creating collection in Mongodb, which is equivalent to tables in SQL

mydb = client["mydatabase"]
mycol = mydb['customers']
print(mydb.list_collection_names())

# insert one record or document
mydict = {"Name": "John", "Address": "Street 37"}
x= mycol.insert_one(mydict)
print(x.inserted_id)
print(client.list_database_names())

# insert multiple record or documents
my_mul_dict = [
    {"_id": 1, "name": "John", "address": "Highway 37"},
    {"_id": 2, "name": "Peter", "address": "Lowstreet 27"},
    {"_id": 3, "name": "Amy", "address": "Apple st 652"},
    {"_id": 4, "name": "Hannah", "address": "Mountain 21"},
    {"_id": 5, "name": "Michael", "address": "Valley 345"},
    {"_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
    {"_id": 7, "name": "Betty", "address": "Green Grass 1"},
    {"_id": 8, "name": "Richard", "address": "Sky st 331"},
    {"_id": 9, "name": "Susan", "address": "One way 98"},
    {"_id": 10, "name": "Vicky", "address": "Yellow Garden 2"},
    {"_id": 11, "name": "Ben", "address": "Park Lane 38"},
    {"_id": 12, "name": "William", "address": "Central st 954"},
    {"_id": 13, "name": "Chuck", "address": "Main Road 989"},
    {"_id": 14, "name": "Viola", "address": "Sideway 1633"}
]
y = mycol.insert_many(my_mul_dict)
print(y.inserted_ids)

# Find command to find one or multiple
xx = mycol.find_one()
print(xx)

for xx in mycol.find():
    print("find without parameter")
    print(xx)

for xx in mycol.find({"_id": 1}):
    print("find with single parameter as a id 1 ")
    print(xx)

for xx in mycol.find({}, {"_id": 0, "name": 1}):
    print("find with two parameter as a id 2 " )
    print(xx)

for xx in mycol.find({"_id": 3}):
    print("find with single parameter as a id 3 ")
    print(xx)

# Query the database
print("Query")
myquery = {"address": "Park Lane 38"}
mydoc = mycol.find(myquery)

for x in mydoc:
    print(x)

#Advance query
print("advance query")
myquery = {"address" : {"$gt" : "S"}}
mydoc = mycol.find(myquery)
for x in mydoc:
    print(x)


# Filter with regular expression
print("regular expression")
myquery = {"address" : {"$regex" : "^S"}}
mydoc = mycol.find(myquery)
for x in mydoc:
    print(x)

# Sort and list out the records documents  in collection
print("sort by name")
mydoc = mycol.find().sort("name")
for x in mydoc:
    print(x)

print("Sort in descending order")
mydoc = mycol.find().sort("name", -1)
for x in mydoc:
    print(x)

#update the record/document

print("Update the record table")
my_query = {"address" : "Valley 345"}
updated_q = {"$set": {"address" : "Canyon 123"}}
x = mycol.update_one(my_query, updated_q)
print(x.modified_count, "modified count")

print("list of all values after update")
for x in mycol.find():
    print(x)

# Update many record values
print("update many")
my_query = {"address" : {"$regex": "^S"}}
updatemany = {"$set": {"name" :"Raaaaaa"}}
xx = mycol.update_many(my_query, updatemany)
print(xx.modified_count)
for x in mycol.find():
    print(x)

# Limit the out of find
print("Limit the number for find")
my_rec = mycol.find().limit(5)
for x in my_rec:
    print(x)


# delete documents, collections, database
print("delete document/record")
myquery = {"address": "Mountain 21"}
mydoc = mycol.delete_one(myquery)

print("delete many documents")
myquery = {"address" : {"$regex" : "^S"}}
mydoc = mycol.delete_many(myquery)
print(mydoc.deleted_count, " deleted count")

print("delete all documents")
x = mycol.delete_many({})
print(x.deleted_count, "deleted count of all documents")

print(mycol.drop(), " dropped the collection/table")

# Reading a csv file and updating the values in database
#creating a Weather database
db1 = client["WeatherDatabase"]
#creating a weather table
mycol1 = db1["Weathercollection"]

#Reading the data from csv file and inserting it in database
BASE_DIR = os.path.join( os.path.dirname( __file__ ), '..' )
pdfile = pd.read_csv(BASE_DIR + "//" + "data" + "//" "input.csv")
records_ = pdfile.to_dict(orient = "records")
result = mycol1.insert_many(records_)

for x in mycol1.find():
    print(x)


#***********************************************************





