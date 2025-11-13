# Databricks notebook source
list = [(1,2), (3,4), (5,6), (7,8)]
print(type(list))
print(type(list[3]))

# COMMAND ----------

df= spark.createDataFrame(list, ('name string , age int'))
display(df)

# COMMAND ----------

user_list = [('Omkar', 30), ('Roja', 50), ('Arun', 60), ('Rojaamma', 70), ('Iya', 20)]
df = spark.createDataFrame(user_list, 'Name string, Age int')
display(df)

# COMMAND ----------

#So we are performing collect() operation on the Dataframe if you see here. See This chamak
df.collect()
print(type(df.collect()))
#Converting a dataframe to list .

# COMMAND ----------

from pyspark.sql import Row
#help(Row)

# COMMAND ----------

row=('Omkar', 'Roja')
type(row)

# COMMAND ----------

row= Row(name='Omkar', where='Roja')

# COMMAND ----------

row.name

# COMMAND ----------

# MAGIC %md
# MAGIC Converting List of Lists into Spark Data Frame using Row

# COMMAND ----------

users_list=[['Omkar', 1], ['Roja', 2], ['Iron_Man', 3], ['Hulk', 4], ['Doctor_Strange', 5], ['Thor',6], ['loki', 7]]
type(users_list)

# COMMAND ----------

type(users_list[3])

# COMMAND ----------

df= spark.createDataFrame(users_list, 'Name string, Id int')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Converting list of list ==> Rows and then converting them into Data Frames 

# COMMAND ----------

users_list=[['Omkar', 1], ['Roja', 2], ['Iron_Man', 3], ['Hulk', 4], ['Doctor_Strange', 5], ['Thor',6], ['loki', 7]]
type(users_list)

# COMMAND ----------

convert_list_row=[Row(*i) for i in users_list]
#You need to understand (Row(*i)) * indicates passing any no of parameters.

# COMMAND ----------

print(convert_list_row)
spark.createDataFrame(convert_list_row, 'Name string, Id int')

# COMMAND ----------

def dummy(*args):
  print(args)
  print(len(args))

# COMMAND ----------

dummy(users_list)
#Observe here it is taking as 1 argument the whole list length but when you give * in front of users_list length will be 7

# COMMAND ----------

dummy(*users_list)

# COMMAND ----------

ls

# COMMAND ----------

# MAGIC %md
# MAGIC Converting list of tuples into Rows and then to the Data Frames

# COMMAND ----------

list = [('Omkar', 1), ('Roja', 2), ('Iron_Man', 3), ('Hulk', 4), ('Doctor_Strange', 5), ('Thor',6), ('loki', 7)]

# COMMAND ----------

type(list)
print(type(list[3]))

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

list_row=(Row(*i) for i in list)

# COMMAND ----------

print(list_row)
df= spark.createDataFrame(list_row)

# COMMAND ----------

display(df)

# COMMAND ----------

def dummy(*args):
  print(args)
  print(len(args))

# COMMAND ----------

dummy(*list)

# COMMAND ----------

# MAGIC %md
# MAGIC Converting List of Dicts into Spark Data Frame using Row.

# COMMAND ----------

user_list=[
  {'user_id' : 1, 'user_first_name' : 'Iron_Man'},
  {'user_id' : 2, 'user_first_name' : 'Bat_Man'},
  {'user_id' : 3, 'user_first_name' : 'Super_Man'},
  {'user_id' : 4, 'user_first_name' : 'Rat_Man'}
]

# COMMAND ----------

type(user_list[3])

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

user_list_row=(Row(i) for i in user_list)
print(user_list_row)

# COMMAND ----------

print(user_list_row)
df = spark.createDataFrame(user_list)
display(df)

# COMMAND ----------

user_details = user_list[1]

# COMMAND ----------

print(type(user_details))
print(user_details)

# COMMAND ----------

user_details.values()

# COMMAND ----------

# MAGIC %md
# MAGIC Basic Types of Data Frames in the Spark

# COMMAND ----------

import datetime
users = [
  {
    "id" : 123,
    "Name" : "Omkar",
    "Address" : "Bengaluru" ,
    "email" : "chevurio@gmail.com" ,
    "customer_from" : datetime.date(2022, 1, 5),
    "last_updated_ts" : datetime.datetime(2022, 2, 5, 1, 15, 0),
    "amount_paid" : 1000.5,
    "Is_Customer" : True
  },
  {
    "id" : 1234,
    "Name" : "Roja",
    "Address" : "Bengaluru" ,
    "email" : "roja@gmail.com" ,
    "customer_from" : datetime.date(2021, 1, 5),
    "last_updated_ts" : datetime.datetime(2022, 3, 5, 1, 16, 0),
    "amount_paid" : 2000.5,
    "Is_Customer" : True
  },
   {
    "id" : 12345,
    "Name" : "Marc",
    "Address" : "California",
    "email" : "dallas@gmail.com",
    "customer_from" : datetime.date(2000, 1, 5),
    "last_updated_ts" : datetime.datetime(2022, 3, 9, 1, 19, 0),
    "amount_paid" : 4000.5,
    "Is_Customer" : False
  },
   {
    "id" : 12346,
    "Name" : "Sing",
    "Address" : "Sikkam",
    "email" : "singh@gmail.com",
    "customer_from" : datetime.date(2021, 1, 5),
    "last_updated_ts" : datetime.datetime(2022, 3, 8, 1, 18, 0),
    "amount_paid" : 6000.5,
    "Is_Customer" : False
  }
]

# COMMAND ----------

type(users[2])

# COMMAND ----------

from pyspark.sql import Row
row_users= (Row(i) for i in users)

# COMMAND ----------

print(row_users)
df=spark.createDataFrame(row_users)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Observe the out put of the data frame to avoid such results you need to create a dataframe as below.

# COMMAND ----------

print(row_users)
df=spark.createDataFrame([Row(**i) for i in users])

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

import datetime
users = [(1,
  'Corrie',
  'Van den Oord',
  'cvandenoord0@etsy.com',
  True,
  1000.55,
  datetime.date(2021, 1, 15),
  datetime.datetime(2021, 2, 10, 1, 15)),
 (2,
  'Nikolaus',
  'Brewitt',
  'nbrewitt1@dailymail.co.uk',
  True,
  900.0,
  datetime.date(2021, 2, 14),
  datetime.datetime(2021, 2, 18, 3, 33)),
 (3,
  'Orelie',
  'Penney',
  'openney2@vistaprint.com',
  True,
  850.55,
  datetime.date(2021, 1, 21),
  datetime.datetime(2021, 3, 15, 15, 16, 55)),
 (4,
  'Ashby',
  'Maddocks',
  'amaddocks3@home.pl',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 10, 17, 45, 30)),
 (5,
  'Kurt',
  'Rome',
  'krome4@shutterfly.com',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 2, 0, 55, 18))]

# COMMAND ----------

users_schema = '''
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_customer BOOLEAN,
    amount_paid FLOAT,
    customer_from DATE,
    last_updated_ts TIMESTAMP
'''

# COMMAND ----------

df=spark.createDataFrame(users, users_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

users_schema = ['id' ,
    'first_name'  ,
    'last_name',
    'email' ,
    'is_customer' ,
    'amount_paid' ,
    'customer_from' ,
    'last_updated_ts' 
    ]

# COMMAND ----------

df=spark.createDataFrame(users, users_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Spark Data Frame using Pandas Data Frame

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

from pyspark.sql import Row
df=spark.createDataFrame([Row(**i) for i in users])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Because of this values are not appropriate in the users list we will first convert them in pandas and then we need to convert them to spark Data frames as Spark cannot create Data Frame with such cases as above

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd.DataFrame(users)

# COMMAND ----------

# Now convert into Spark data Frames.
spark.createDataFrame(pd.DataFrame(users)).show

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": ["+1 234 567 8901", "+1 234 567 8911"],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers": ["+1 234 567 8923", "+1 234 567 8934"],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": ["+1 714 512 9752", "+1 714 512 6601"],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": ["+1 817 934 7142"],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

