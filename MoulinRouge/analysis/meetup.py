import pandas as pd
from pymongo import MongoClient

import matplotlib.pyplot as plt
import networkx as nx

from networkx.algorithms import community  as nxcom
def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]


def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    return df

df = read_mongo(db="heartbeat", collection="meetup")

unique_groups = df["group_name"].unique()
color_map = ["blue"] * len(unique_groups)


group_names = unique_groups
person_group = []
edges = []

# ---------------------------------------------------------------------------------
for g in unique_groups:
    group_events = df[df.group_name == g]
    
    for index, row in group_events.iterrows():
        event_attendees = row['event_attendees']

        for person in event_attendees:
            #print(person["name"])

            rel_person_group = {
                "person_id":person["id"],
                "group_name":row["group_name"]
            }
            rel = (person["id"], row["group_name"])
            edges.append(rel)
            
            person_group.append(person["id"])

# ---------------------------------------------------------------------------------


unique_edges = list(dict.fromkeys(person_group))


print("Total number of groups detected: "+ str(len(unique_groups)))
print("Total number of people involved in Meetup Paris at this time: " + str(len(unique_edges)))

unique_edges_colors = ["green"] * len(unique_edges)
color_map += unique_edges_colors

G = nx.Graph()
G.add_nodes_from(unique_groups, node_color='r')
G.add_edges_from(edges)  

# groups with more events / locations with more events
top_groups = df['group_name'].value_counts()[:10].index.tolist()
top_locations = df['event_location'].value_counts()[:10].index.tolist()
print(top_locations)

# groups that mobilize the most people. Agg sum of attendees in all available events
df['num_attendees']  = (df['event_attendees'].str.len())

r = df.groupby(['group_name'])["num_attendees"].agg('sum').reset_index()
r = r.sort_values('num_attendees', ascending=False)
print(r)

# Get percentages of men/women per event
def attendees_women(row, gender="women"):
    num_total = 0
    num_men = 0
    num_wom = 0
    for a in row["event_attendees"]:
        num_total += 1
        if a["gender"] == "male":
            num_men += 1
        elif a["gender"] == "female":
            num_wom += 1

    per_wom = num_wom / num_total
    per_men = num_men / num_total
    if gender == "women":
        return per_wom
    elif gender == "men":
        return per_men


df['attendees_women'] = df.apply(lambda row: attendees_women(row), axis=1)
df['attendees_men'] = df.apply(lambda row: attendees_women(row, gender="men"), axis=1)
print("Events with higher % women")
print(df[df["num_attendees"]>50].sort_values('attendees_women',ascending=False))


print(top_locations)

#df['attendees_women']  = (df['event_attendees'].str.len())
#df['attendees_men']  = (df['event_attendees'].str.len())

communities = sorted(nxcom.greedy_modularity_communities(G), key=len, reverse=True)
    # Count the communities
print(f"The meetup Paris network has {len(communities)} communities.")  
"""

# Definition: Betweenness centrality measures the number of times a node lies on the shortest path between other nodes.
print("Betweenness centrality")
betCent = nx.betweenness_centrality(G, normalized=True, endpoints=True)
influencers = [ elem for elem in betCent if elem not in unique_groups] 
calced = (sorted(influencers, key=betCent.get, reverse=True))
top_influencers = (calced[:5])
for influencer in top_influencers:
    print("User id: "+str(influencer)+" Betweenness centrality coef: "+str(betCent.get(influencer)))


# Definition: Betweenness centrality measures the number of times a node lies on the shortest path between other nodes.
print("Degree centrality")
betCent = nx.degree_centrality(G)
influencers = [ elem for elem in betCent if elem not in unique_groups] 
calced = (sorted(influencers, key=betCent.get, reverse=True))
top_influencers = (calced[:5])
for influencer in top_influencers:
    print("User id: "+str(influencer)+" Degree centrality coef: "+str(betCent.get(influencer)))
    #print("User id: "+str(influencer)+" Connections: ")
    #print(str(G.edges(influencer)))

# EigenCentrality measures a node’s influence based on the number of links it has to other nodes in the network. EigenCentrality then goes a step further by also taking into account how well connected a node is, and how many links their connections have, and so on through the network.
# By calculating the extended connections of a node, EigenCentrality can identify nodes with influence over the whole network, not just those directly connected to it.
print("Eigenvector centrality")
betCent = nx.eigenvector_centrality(G)
influencers = [ elem for elem in betCent if elem not in unique_groups] 
calced = (sorted(influencers, key=betCent.get, reverse=True))
top_influencers = (calced[:5])
for influencer in top_influencers:
    print("User id: "+str(influencer)+" Eigenvector centrality coef: "+str(betCent.get(influencer)))
    #print("User id: "+str(influencer)+" Connections: ")
    #print(str(G.edges(influencer)))


# Closeness centrality scores each node based on their ‘closeness’ to all other nodes in the network.
# This measure calculates the shortest paths between all nodes, then assigns each node a score based on its sum of shortest paths.
print("Closeness centrality")
betCent = nx.closeness_centrality(G)
influencers = [ elem for elem in betCent if elem not in unique_groups] 
calced = (sorted(influencers, key=betCent.get, reverse=True))
top_influencers = (calced[:5])
for influencer in top_influencers:
    print("User id: "+str(influencer)+" Closeness centrality coef: "+str(betCent.get(influencer)))
    #print("User id: "+str(influencer)+" Connections: ")
    #print(str(G.edges(influencer)))

#nx.draw(G,node_color=color_map, with_labels = False)


#plt.show()
#plt.show()

"""
