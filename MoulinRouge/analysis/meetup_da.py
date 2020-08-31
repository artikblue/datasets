import pandas as pd
import urllib, base64
import dash
import dash_core_components as dcc
import dash_html_components as html
from pymongo import MongoClient
from datetime import datetime
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import nltk 
from nltk import tokenize
from nltk.corpus import stopwords
import plotly.graph_objs as go
import plotly.express as px
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot

import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import networkx as nx
from networkx.algorithms import community  as nxcom
import plotly.graph_objects as go

import io

import db_helper as mongodb
class MeetupDa:

    def __init__(self, city, lang):

        self.city = city
        self.lang = lang

        # objects needed

        # All events recorded from the meetup scence in a city
        # List of unique groups detected
        self._load_meetup_dataframe()
        self._filter_meetup_dataframe()
        
        # All of the people involved in the city (people-event 1-1 relation)
        # Graph linking people-group
        self._generate_graph()
    
    # LOADING & FILTERING
    def _load_meetup_dataframe(self, db="heartbeat", collection="meetup"):
        # filter values for a particular city!
        db = mongodb.MongoHelper()
        self.meetup_dataframe = db.read_mongo()

    # initial filtering
    def _filter_meetup_dataframe(self):
        # calculate the duration of an event
        self.meetup_dataframe["event_duration"] = self.meetup_dataframe.apply(lambda row:  datetime.strptime(row.event_hour_end, "%H:%M") - datetime.strptime(row.event_time, "%H:%M:%S") , axis = 1) 
        # calculate the number of attendees per event
        self.meetup_dataframe['num_attendees']  = (self.meetup_dataframe['event_attendees'].str.len())
        # calculate the number of comment per event
        self.meetup_dataframe['num_comments']  = (self.meetup_dataframe['event_comments'].str.len())
        # generate the list of unique groups
        self.unique_groups = self.meetup_dataframe["group_name"].unique()
    
    # GRAPH GENERATION
    def _generate_graph(self):
        # generates a dataframe containing 1-1 person-event where the person is the id
        # generates a graph connecting persons-groups
        edges = []
        persons = []
        for g in self.unique_groups:
            group_events = self.meetup_dataframe[self.meetup_dataframe.group_name == g]
            
            for index, row in group_events.iterrows():
                event_attendees = row['event_attendees']

                for person in event_attendees:
                    person_obj = {
                        "id":person["id"], 
                        "name":person["name"], 
                        "group":row["group_name"],
                        "event":row["event_title"],
                        "profile":person["web_actions"]["group_profile_link"], 
                        "gender":person["gender"],
                        "coords":row["coords"],
                        "event":row["event_title"],
                        "event_url":row["event_url"]
                    }
                    persons.append(person_obj)
                        
                    rel = (person["id"], row["group_name"])
                    edges.append(rel)

        self.persons_dataframe = pd.DataFrame(persons)            
        self.G = nx.Graph()
        self.G.add_nodes_from(self.unique_groups, node_color='r')
        self.G.add_edges_from(edges)

    # generate (plotly) graph plot
    def get_graph(self):
        return self.G

    def gen_graph_plot(self):
        # This function should be reviewed and optimized for that I suspect that this can be re-coded in a more elegant and optimal way
        pass

    #/ GRAPH ANALYSIS
    # analyse the graph to identify communities in it
    def _analyse_communities(self):
        self.communities = sorted(nxcom.greedy_modularity_communities(self.G), key=len, reverse=True)
        return self.communities

    def get_communities(self):
        if hasattr(self, 'communities'):
            return self.communities
        else:
            self._analyse_communities()
            return self.communities
    
    def get_communities_len(self):
        return len(self.get_communities())

    # user info by id
    def _get_user_by_id(self, id):
        user_data = self.persons_dataframe[self.persons_dataframe["id"]==id]

        influencer_obj = {
            "name":user_data.iloc[0]["name"],
            "profile":user_data.iloc[0]["profile"],
            "gender":user_data.iloc[0]["gender"]
        }
        return influencer_obj
    
    # EigenCentrality measures a node’s influence based on the number of links it has to other nodes in the network. EigenCentrality then goes a step further by also taking into account how well connected a node is, and how many links their connections have, and so on through the network.
    # By calculating the extended connections of a node, EigenCentrality can identify nodes with influence over the whole network, not just those dir
    # graph analysis measures 
    def get_coefs(self, limit=5, coef="betweenness_centrality"):
        if coef == "betweenness_centrality":
            coef_res = nx.betweenness_centrality(self.G)

        elif coef == "eigenvector_centrality":
            coef_res = nx.eigenvector_centrality(self.G)

        elif coef == "degree_centrality":
            coef_res = nx.degree_centrality(self.G)
        
        elif coef == "closeness_centrality":
            coef_res = nx.closeness_centrality(self.G)

        influencers = [ elem for elem in coef_res if elem not in self.unique_groups] 
        calced = (sorted(influencers, key=coef_res.get, reverse=True))
        top_influencers = (calced[:limit])
        top_influencers_obj = []
        for influencer in top_influencers:
            influencer_obj = self._get_user_by_id(influencer)
            influencer_obj[coef] = coef_res.get(influencer)
            
            top_influencers_obj.append(influencer_obj)

        return top_influencers_obj

    # UNIQUE VALUES
    def get_unique_groups(self):
        return self.unique_groups

    def get_unique_groups_len(self):
        return len(self.unique_groups)

    def get_unique_events(self):
        return self.meetup_dataframe["event_title"].unique()
    
    def get_unique_events_len(self):
        return len(self.meetup_dataframe["event_title"].unique())

    def get_unique_users(self):
        return self.persons_dataframe["id"].unique()

    def get_unique_users_len(self):
        return len(self.persons_dataframe["id"].unique())
    # GENERAL STAT CALCULATIONS
    # % gender
    def get_gender_percentages(self):
        genders=(self.persons_dataframe['gender'].value_counts(normalize=True) * 100).reset_index()
        genders=genders.rename(columns={"index":"gender","gender":"num" })
        return genders
    
    
    def calc_gender_avg(self,v, gender):
        return len(v[v==gender])/len(v)
    # get % of participation by sex by event
    def calc_events_gender(self):
        # lambda functions == your friends!
        r = self.persons_dataframe.groupby(['event','event_url']).agg(
            male=('gender',lambda row: self.calc_gender_avg(row, gender="male")),
            female=('gender', lambda row: self.calc_gender_avg(row, gender="female")),
            participants=('gender', 'count')).reset_index()
        return r
    # get top <limit> events of higher <gender>% gender participation
    def get_events_gender(self, gender="female", limit=5, min_participation=10.0):
        r = self.calc_events_gender()
        r = r[r["participants"]>min_participation]
        return r.sort_values(gender,ascending=False)[["event","event_url",gender]][:limit]


    # groups that are producing more events
    def top_groups(self, limit=5):
        tgroups = self.meetup_dataframe['group_name'].value_counts()[:limit].reset_index()

        return tgroups
    # locations that hold more events
    def top_locations(self, limit=5):
        tlocations = self.meetup_dataframe['event_location'].value_counts()[:limit].index.tolist()
        return tlocations
    # users that are participating in more events
    def top_users(self, limit=5):
        tusers = self.persons_dataframe['id'].value_counts()[:limit].index.tolist()
        return tusers[:5]
    # groups that mobilzie more people
    def top_groups_people(self, limit=5):
        groups_people = self.meetup_dataframe.groupby(['group_name'])["num_attendees"].agg('sum').reset_index()
        groups_people = groups_people.sort_values('num_attendees', ascending=False)
        return groups_people[:limit]

    # groups that generate the most comments
    def top_groups_comments(self, limit=5):
        groups_comments = self.meetup_dataframe.groupby(['group_name'])["num_comments"].agg('sum').reset_index()
        groups_comments = groups_comments.sort_values('num_comments', ascending=False)
        return groups_comments[:limit]

    # start hours that appear to be most common
    def top_start_hours(self, limit=5):
        top_timeframes = self.meetup_dataframe.groupby(['event_time'])["event_id"].count().to_frame('count').reset_index()
        top_timeframes = top_timeframes.sort_values("count",ascending=False)
        return top_timeframes[:limit]

    # timeframes (durations) that appear to be most common
    def top_timeframes(self, limit=5):
        top_durations = self.meetup_dataframe.groupby(['event_duration'])["event_id"].count().to_frame('count').reset_index()
        top_durations = top_durations.sort_values("count",ascending=False)
        return top_durations[:limit]        

    # TEXT ANALYSIS

    def _get_top_words_event(self, field="event_desc", limit = 5):
        # returns top <limit> of most common words of lenght > 4
        # fields can be event_desc for the description / event_title for the title
        common = pd.Series(' '.join(self.meetup_dataframe['event_desc'].apply(lambda i: ' '.join(filter(lambda j: len(j) > 4, i.split())))).lower().split()).value_counts()[:limit]
        return common

    def _load_stopwords(self):
        stopwords_fr = set(stopwords.words(self.lang))
        # always take english into account
        stopwords_en = set(stopwords.words('english'))
        spwords = stopwords_fr | stopwords_en

        return spwords

    def gen_wordcloud(self, field="event_title"):
        # at this moment fields can be: event_title for a wordcloud of event titles
        # or event_desc for a wordcloud made of event description words
        # returns a base64 img that can be easily embedded in dash
        text = " ".join(t for t in self.meetup_dataframe[field])
        
        wordcloud = WordCloud(width=400, height=400, stopwords=self._load_stopwords(),background_color="white").generate(text)
        return wordcloud

    # MAPS / GEOLOCATION ANALYSIS
    

    def _get_nonempty_coords(self):
        r = self.meetup_dataframe[self.meetup_dataframe['coords'].str.len() > 0]
        r[['lat','lon']] = pd.DataFrame(r['coords'].tolist(),index=r.index)
        return r
    # get list of coords that appear the most (or group coords by number of times they appear...)
    def get_top_coords(self):
        nonempty_coords = self._get_nonempty_coords()
        top_coords = nonempty_coords.groupby(nonempty_coords['coords'].map(tuple))['event_id'].count().to_frame('count').reset_index()
        top_coords[['lat','lon']] = pd.DataFrame(top_coords['coords'].tolist(),index=top_coords.index)

        return top_coords
    # generate a density map / heatmap showing areas that hold most events
    def gen_density_map(self):
        top_coords = self.get_top_coords()
        # TO-DO: must parametrize lat,lon coords or retrieve them automatically
        fig = px.density_mapbox(top_coords, lat="lat", lon="lon", z="count", radius=12,
                        center=dict(lat=48.864, lon=2.349), zoom=9)

        fig.update_layout(mapbox_style="dark", mapbox_accesstoken=self.mapbox_at)

        return fig
    # map events, the higher the circle the more participants
    def gen_events_geo(self):
        df_geo = self._get_nonempty_coords()
        df_geo[['lat','lon']] = pd.DataFrame(df_geo['coords'].tolist(),index=df_geo.index)
        return df_geo

    # filters the user - event dataframe by user
    def _filter_user_df(self, particular_user):
        user_df = self.persons_dataframe["id"] == particular_user
        return user_df

    # returns a tuple containing all events user will take part and also all of user groups
    def user_events_groups(self, particular_user):
        user_df = self._filter_user_df(particular_user)

        user_events = user_df["event"].unique()
        user_groups = user_df["group"].unique()
        return user_events, user_groups


    # map zones where the muser moves more
    def get_user_zones(self, userid):
        # event - user relation for user==userid and with coords nonempty
        target_df = self.persons_dataframe[ (self.persons_dataframe["id"] == particular_user) & (self.persons_dataframe['coords'].str.len() > 0)]
        target_zone = target_df.groupby(target_df['coords'].map(tuple))['id'].count().to_frame('count').reset_index()
        target_zone[['lat','lon']] = pd.DataFrame(target_zone['coords'].tolist(),index=target_zone.index)
        fig = px.density_mapbox(target_zone, lat="lat", lon="lon", z="count", radius=12,
                        center=dict(lat=48.864, lon=2.349), zoom=9)
        return fig

    
