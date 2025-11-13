# Databricks notebook source
import pandas as pd
df = pd.read_csv('s3://tfsdl-corp-training-test/Rom/workspace/user_Records.csv')
pd.DataFrame(df)

# COMMAND ----------

#Replacing the spaces in the column names with under score
df.columns = df.columns.str.replace(' ', '_')
pd.DataFrame(df)

# COMMAND ----------

users=spark.createDataFrame(pd.DataFrame(df))

# COMMAND ----------

type(users)

# COMMAND ----------

users.printSchema()

# COMMAND ----------

users.createOrReplaceTempView("users")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Query: Extract Doctor details.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from users where Name_Prefix in ('Dr.')

# COMMAND ----------

# MAGIC %md
# MAGIC Query:Segregate the data with respect to gender in different file

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Male_Gender AS
# MAGIC Select Emp_ID,Name_Prefix,First_Name,Middle_Initial,
# MAGIC CASE
# MAGIC     WHEN Gender = 'M' THEN 'Male'
# MAGIC     WHEN Gender = 'F' THEN 'Female'
# MAGIC     ELSE 'NA'
# MAGIC END AS Gender
# MAGIC FROM users where Gender = 'M';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Female_Gender AS 
# MAGIC Select Emp_ID,Name_Prefix,First_Name,Middle_Initial,
# MAGIC CASE
# MAGIC     WHEN Gender = 'M' THEN 'Male'
# MAGIC     WHEN Gender = 'F' THEN 'Female'
# MAGIC     ELSE 'NA'
# MAGIC END AS Gender
# MAGIC FROM users where Gender = 'F';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Female_Gender;

# COMMAND ----------

# MAGIC %md
# MAGIC Query:Find out who are all having minimum 14% hike

# COMMAND ----------

# MAGIC %sql
# MAGIC --Select * from users where 'Last_%_Hike' in ('14%')
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM users  
# MAGIC WHERE 'Last_%_Hike' IN  
# MAGIC ( SELECT 'Last_%_Hike'
# MAGIC FROM users  
# MAGIC where Last_%_Hike <= 14% GROUP BY Last_%_Hike 
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Query:Statewise store the user details

# COMMAND ----------

# MAGIC %sql
# MAGIC select users.State, users.Emp_ID, users.First_Name from users,
# MAGIC (select 
# MAGIC State, 
# MAGIC Emp_ID,
# MAGIC First_Name,
# MAGIC Middle_Initial
# MAGIC from users 
# MAGIC group by users.State, Emp_ID, First_Name, Middle_Initial
# MAGIC ) result1
# MAGIC WHERE result1.State = users.State 

# COMMAND ----------

# MAGIC %sql
# MAGIC select Emp_ID, State, First_Name, 
# MAGIC    dense_rank() over ( partition by State order by Emp_ID desc ) rn
# MAGIC  from users ;

# COMMAND ----------

# MAGIC %md
# MAGIC Query:Load the user details who were joined in between 2010 to 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users where Year_of_Joining between '2010' and '2016'

# COMMAND ----------

# MAGIC %md
# MAGIC Query: Load the user data with respect to region

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create table region_user_data like users
# MAGIC INSERT INTO region_user_data (County, Region)
# MAGIC SELECT users.County, users.Region, dense_rank() over ( partition by State order by Emp_ID desc ) rn
# MAGIC  from users ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC INTO regiondata,
# MAGIC FROM users
# MAGIC WHERE Region = 'Midwest'

# COMMAND ----------

# MAGIC %md
# MAGIC Query:List the number of users who are coming from each city

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t2.City, t2.User_Name
# MAGIC     ,count(t1.City) AS Users_from_city
# MAGIC FROM Users t1
# MAGIC INNER JOIN Users t2 ON t1.City = t2.City
# MAGIC GROUP BY t2.City, t2.User_Name

# COMMAND ----------

# MAGIC %sql
# MAGIC select City, User_Name, count(*) from users where User_Name='jwrobinson' group by City ,User_Name

# COMMAND ----------

