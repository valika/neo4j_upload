import pandas as pd

#upload file
artist=pd.read_csv('albumlist.csv', sep=",", header=0,encoding='latin-1')
artist.head()

#upload database for connection to neo4j
from py2neo import Graph
#IP Address
graph = Graph("bolt://192.168.1.101:7687")

#creation of unique list of Artist
list_Artist=[]

for i,row in enumerate(artist['Artist']):
    if row not in list_Artist:
        list_Artist.append(row)     

#Creation of nodes and dependency in the graph neo4j
tx = graph.begin()
for name in list_Artist:
    tx.run("CREATE (p:Person {name:{name}}) RETURN p", name=name) 
for name in artist['Album']:
    tx.run("CREATE(a:Album{name:{name}}) RETURN a",name=name)
    
for index,row in artist[['Artist','Album']].iterrows():
     tx.evaluate('''
       MATCH (p:Person {name:$Person}), (a:Album {name:$Am})
       MERGE (p)-[pr:PRODUCED]->(a)
       ''', parameters = {'Person': row[0], 'Am': row[1]})   
tx.commit()  

print("Done") 