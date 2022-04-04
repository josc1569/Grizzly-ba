import grizzly
import pandas
from grizzly.relationaldbexecutor import RelationalExecutor
from grizzly.sqlgenerator import SQLGenerator
import sqlite3

con = sqlite3.connect("grizzly.db")
grizzly.use(RelationalExecutor(con, SQLGenerator("sqlite")))
df1 = grizzly.read_table("events")

df1 = df1[['theyear', 'numarticles']]
df1 = df1[df1.theyear == 2014]
df1.show(pretty=True)

df = df1.diff(mode='pandas')
print(df)














'''grobe Umsetzung von der Client-Side Execution Engine mit DuckDB
print('Umsetzung der Client-Side Execution Engine mit DuckDB:')
import duckdb

#create an in-memory temporary database
conn = duckdb.connect()

#create a relation for every dataframe of the operation
rel = conn.from_df(df.sql_to_dataframe(con1))
rel1 = conn.from_df(df1.sql_to_dataframe(con1))
rel2 = conn.from_df(df2.sql_to_dataframe(con2))

#exceute the operation
res = rel.filter('theyear = 2014').aggregate("count(theyear)")

print(type(res), res)
'''



'''Umsetzungsmöglichkeit von ApacheArrow
print('Umsetzung der Client-Side Execution Engine mit ApacheArrow:')
import pyarrow as pa

table1 = pa.Table.from_pandas(df.sql_to_dataframe(con1))

res = table1.group_by("theyear").aggregate([
    ("theyear", "count", pa.compute.CountOptions(mode="all"))
     ])

print(type(res), res)
'''
