import grizzly
import pandas
from grizzly.relationaldbexecutor import RelationalExecutor
from grizzly.sqlgenerator import SQLGenerator
import sqlite3

con = sqlite3.connect("grizzly.db")
grizzly.use(RelationalExecutor(con, SQLGenerator("sqlite")))
df = grizzly.read_table("events")

df1 = df[['globaleventid', 'actor1name', 'numarticles']]
df2 = df[['globaleventid', 'actor2name', 'numarticles']]

pdf1 = df1.sql_to_dataframe()
pdf2 = df2.sql_to_dataframe()

import duckdb
con = duckdb.connect()
rel1 = con.from_df(pdf1)
rel2 = con.from_df(pdf2)
res = con.execute("SELECT * FROM pdf1").fetchdf()


print(df1.merge(mode='duckdb', objs=df2))




'''pdf = df1.merge(mode='duckdb', objs=df2)
#print(type(pdf), '\n', pdf)

print(df1.concat_tables(mode='pyarrow', tables=df2))
df = df1.assign(test_numart=lambda x: x.numarticles * 20)
print(df)

pdf2 = pdf.merge(right=df)
print(pdf2)'''