import grizzly
import pandas
from grizzly.relationaldbexecutor import RelationalExecutor
from grizzly.sqlgenerator import SQLGenerator
import sqlite3

con = sqlite3.connect("grizzly.db")
grizzly.use(RelationalExecutor(con, SQLGenerator("sqlite")))
df = grizzly.read_table("events")

'''Beispiel der merge Funktion mit Hilfe auf Basis von Pandas'''
print('Beispiel der merge Funktion mit Hilfe auf Basis von Pandas')
df1 = df[["globaleventid", "actor1name", "numarticles"]]
df2 = df[["globaleventid", "actor1name", "actor2name", "fractiondate"]]
merged = df2.merge(other=df1,mode='pandas', left_on='actor1name', right_on='actor1name')
print(type(merged), merged)
print(merged.columns)
print('_____________________________________________________')
df3 = df[df["globaleventid"] == 470747760] # filter
df3 = df3[["globaleventid", "actor1countrycode", "actor2countrycode"]]
print(type(df3))
df3.show(pretty=True)
print('_____________________________________________________')
print(df3.merge(other=(df2.merge(other=df1, mode='pandas', left_on='actor1name', right_on='actor1name')),mode='pandas', left_on='globaleventid', right_on='globaleventid_x', how='left'))


'''Beispiel der merge Funktion auf Basis von DuckDB'''
print('Beispiel der merge Funktion mit Hilfe auf Basis von DuckDB')
df1 = df[["globaleventid", "actor1name", "numarticles"]]
df2 = df[["globaleventid", "actor1name", "actor2name", "fractiondate"]]
merged = df2.merge(other=df1,mode='duckdb', left_on='actor1name', right_on='actor1name')
print(type(merged), merged)
print('_____________________________________________________')
df3 = df[df["globaleventid"] == 470747760] # filter
df3 = df3[["globaleventid", "actor1countrycode", "actor2countrycode"]]
print(type(df3))
df3.show(pretty=True)
print('_____________________________________________________')
print(df3.merge(other=(df2.merge(other=df1, mode='duckdb', left_on='actor1name', right_on='actor1name')), mode='duckdb', on='globaleventid', how='left'))