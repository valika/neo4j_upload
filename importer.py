import pandas as pd

artist=pd.read_csv('albumlist.csv', sep=",", header=0,encoding='latin-1')
artist.head()

from py2neo import Graph
graph = Graph("bolt://192.168.1.101:7687")


tx = graph.begin()
for name in artist['Artist']:
    tx.run("CREATE (p:Person {name:{name}}) RETURN p", name=name) 
for name in artist['Album']:
    tx.run("CREATE(a:Album{name:{name}}) RETURN a",name=name)
    
for index,row in artist[['Artist','Album']].iterrows():
     tx.evaluate('''
       MATCH (p:Person {name:$Person}), (a:Album {name:$Am})
       MERGE (p)-[pr:PRODUCED]->(a)
       ''', parameters = {'Person': row[0], 'Am': row[1]})   
tx.commit()   