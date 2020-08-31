from dash.dependencies import Input, Output, State
from pymongo import MongoClient
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go 
import numpy as np 
import requests, json
import pandas as pd
import plotly.express as px
import networkx as nx
import matplotlib.pyplot as plt
from os import path
from PIL import Image
import io
import urllib, base64

import meetup_da as service

import plotly.graph_objects as go

import db_helper as mongodb

import pickle
class MeetupGraph:

    def __init__(self, city, lang):
        self.city = city
        self.lang = lang

        self.data_service = service.MeetupDa("Paris", "french")
        self.mapbox_at = "pk.eyJ1IjoiY3VyYXNhbzk5IiwiYSI6ImNrZTl2eTJpeDJhaDEzN243YXdjY24zN20ifQ.cRm0go3r-tdZdTU-GBQitw"

        self.data_added = False

        self._load_dashboard_data()
        
        
    # we save/load previously generated results to avoid repetitive time consuming operations
    def _load_dashboard_data(self):
        self.db = mongodb.MongoHelper()
        db_data = self.db.read_dashboard_data()
        if db_data:
            self.dashboard_data = db_data
        else:
            self.dashboard_data = {}
    
    def save_dashboard_data(self):
        if self.data_added:
            self.db.save_dashboard_data(dashboard_data=self.dashboard_data)
    # HEAT MAPS
    def get_heatmap(self):
        if "heatmap_coords" in self.dashboard_data:
            top_coords = pickle.loads(self.dashboard_data["heatmap_coords"])
        else:
            top_coords = self.data_service.get_top_coords()
            self.dashboard_data["heatmap_coords"] = pickle.dumps(top_coords)
            self.data_added = True

        layout = go.Layout(
            margin=go.layout.Margin(
                    l=0, #left margin
                    r=0, #right margin
                    b=0, #bottom margin
                    t=0, #top margin
                )
            )

        fig = px.density_mapbox(top_coords, lat="lat", lon="lon", z="count", radius=12,
                        center=dict(lat=48.864, lon=2.349), zoom=9)

        fig.update_layout(mapbox_style="dark", mapbox_accesstoken=self.mapbox_at)
        
        return fig

    def get_user_heatmap(self, user):
        pass

    # EVENT MAPS
    def get_eventsmap(self):
        if "event_coords" in self.dashboard_data:
            df_geo = pickle.loads(self.dashboard_data["event_coords"])
        else:
            df_geo = self.data_service.gen_events_geo()
            self.dashboard_data["event_coords"] = pickle.dumps(df_geo)
            self.data_added = True

        
        fig = px.scatter_mapbox(df_geo, lat="lat", lon="lon", color="group_name", size="num_attendees",
                  color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, center=dict(lat=48.864, lon=2.349), zoom=12)
        
        fig.update_layout(mapbox_style="dark", mapbox_accesstoken=self.mapbox_at)

        return fig

    
    def get_user_eventsmap(self, user):
        pass
    
    # NETWORK DIAGRAMS
    def get_network(self):
        if "graph" in self.dashboard_data:
            G = pickle.loads(self.dashboard_data["graph"])
        else:
            G = self.data_service.get_graph()
            self.dashboard_data["graph"] = pickle.dumps(G)
            self.data_added = True

        # if we serialzie and load/store the whole fig
        # we'll optimize the load process for 10 seconds at least, maybe even more
        pos = nx.spring_layout(G, k=0.05)
        for n, p in pos.items():
            G.nodes[n]['pos'] = p

        edge_trace = go.Scatter(
            x=[],
            y=[],
            line=dict(width=0.5,color='#888'),
            hoverinfo='none',
            mode='lines')

        for edge in G.edges():
            x0, y0 = G.nodes[edge[0]]['pos']
            x1, y1 = G.nodes[edge[1]]['pos']
            edge_trace['x'] += tuple([x0, x1, None])
            edge_trace['y'] += tuple([y0, y1, None])

        node_trace = go.Scatter(
            x=[],
            y=[],
            text=[],
            mode='markers',
            hoverinfo='text',
            marker=dict(
                showscale=True,
                colorscale='RdBu',
                reversescale=True,
                color=[],
                size=8,
                colorbar=dict(
                    thickness=10,
                    title='Node Connections',
                    xanchor='left',
                    titleside='right'
                ),
                line=dict(width=0)))

        for node in G.nodes():
            x, y = G.nodes[node]['pos']
            node_trace['x'] += tuple([x])
            node_trace['y'] += tuple([y])    

        for node, adjacencies in enumerate(G.adjacency()):
            node_trace['marker']['color']+=tuple([len(adjacencies[1])])
            node_info = str(adjacencies[0]) +' # of connections: '+str(len(adjacencies[1]))
            node_trace['text']+=tuple([node_info])

        fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='<br>Meetup Paris network',
                        titlefont=dict(size=16),
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20,l=5,r=5,t=40),
                        annotations=[ dict(
                            text="No. of connections",
                            showarrow=False,
                            xref="paper", yref="paper") ],
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
        return fig
    
    def get_user_network(self):
        pass
    
    # WORDCLOUD
    def get_wordcloud(self,field="event_title"):
        if "wordcloud" in self.dashboard_data:
            wordcloud = pickle.loads(self.dashboard_data["wordcloud"])
        else:
            wordcloud = self.data_service.gen_wordcloud(field)
            self.dashboard_data["wordcloud"] = pickle.dumps(wordcloud)
            self.data_added = True
        
        plt.imshow(wordcloud)
        plt.axis("off")

        image = io.BytesIO()
        plt.savefig(image, format='png')
        image.seek(0)  # rewind the data
        string = base64.b64encode(image.read())

        image_64 = 'data:image/png;base64,' + urllib.parse.quote(string)
        return image_64

    # GENERAL STATISTICS
    def get_generalstats(self):

        if "general_stats" in self.dashboard_data:
            general_stats = self.dashboard_data["general_stats"]
            num_groups = pickle.loads(general_stats["num_groups"])
            num_events = pickle.loads(general_stats["num_events"])
            num_users = pickle.loads(general_stats["num_users"])
            num_communities = pickle.loads(general_stats["num_communities"])
        else:
            num_groups = self.data_service.get_unique_groups_len()
            num_events = self.data_service.get_unique_events_len()
            num_users = self.data_service.get_unique_users_len()
            num_communities = self.data_service.get_communities_len()
            general_stats_obj = {
                "num_groups": pickle.dumps(num_groups),
                "num_events": pickle.dumps(num_events),
                "num_users": pickle.dumps(num_users),
                "num_communities": pickle.dumps(num_communities)
            }
            self.dashboard_data["general_stats"] = general_stats_obj
            self.data_added = True

        

        general_stats = html.Div([
            html.Table([
                html.Tr(
                    [html.Td('Number of groups'), html.Td(num_groups)]
                ),
                html.Tr(
                    [html.Td('Number of events'), html.Td(num_events)]
                ),
                html.Tr(
                    [html.Td('Number of users'), html.Td(num_users)]
                ),
                html.Tr(
                    [html.Td('Number of detected communities'), html.Td(num_communities)]
                )
            ])
        ])

        return general_stats

    # INFLUENCERS
    def get_influencers(self):
        if "influencers" in self.dashboard_data:
            influencers = self.dashboard_data["influencers"]
            coefs_bc = pickle.loads(influencers["coefs_bc"])
            coefs_ec = pickle.loads(influencers["coefs_ec"])
            coefs_dc = pickle.loads(influencers["coefs_dc"])
            coefs_cc = pickle.loads(influencers["coefs_cc"])
        else:
            coefs_bc = self.data_service.get_coefs()
            coefs_ec = self.data_service.get_coefs(coef="eigenvector_centrality")
            coefs_dc = self.data_service.get_coefs(coef="degree_centrality")
            coefs_cc = self.data_service.get_coefs(coef="closeness_centrality")
            influencers_obj = {
                "coefs_bc": pickle.dumps(coefs_bc),
                "coefs_ec": pickle.dumps(coefs_ec),
                "coefs_dc": pickle.dumps(coefs_dc),
                "coefs_cc": pickle.dumps(coefs_cc)
            }
            self.dashboard_data["influencers"] = influencers_obj
            self.data_added = True

        
        bc_divs = [html.Tr([html.Th("User"), html.Th("Gender"), html.Th("Betweenness centrality")])]
        for i in coefs_bc:
            row = html.Tr([html.Td(html.A(href=i["profile"], children=i["name"])), html.Td(i["gender"]), html.Td(round(i["betweenness_centrality"],6))])
            bc_divs.append(row)
        
        bc_table = html.Div([
            html.Table(bc_divs)
        ])

        ec_divs = [html.Tr([html.Th("User"), html.Th("Gender"), html.Th("Eigenvector centrality")])]
        for i in coefs_ec:
            row = html.Tr([html.Td(html.A(href=i["profile"], children=i["name"])), html.Td(i["gender"]), html.Td(round(i["eigenvector_centrality"],6))])
            ec_divs.append(row)
        
        ec_table = html.Div([
            html.Table(ec_divs)
        ])

        dc_divs = [html.Tr([html.Th("User"), html.Th("Gender"), html.Th("Degree centrality")])]
        for i in coefs_dc:
            row = html.Tr([html.Td(html.A(href=i["profile"], children=i["name"])), html.Td(i["gender"]), html.Td(round(i["degree_centrality"],6))])
            dc_divs.append(row)
        
        dc_table = html.Div([
            html.Table(dc_divs)
        ])

        cc_divs = [html.Tr([html.Th("User"), html.Th("Gender"), html.Th("closeness centrality")])]
        for i in coefs_cc:
            row = html.Tr([html.Td(html.A(href=i["profile"], children=i["name"])), html.Td(i["gender"]), html.Td(round(i["closeness_centrality"],6))])
            cc_divs.append(row)
        
        cc_table = html.Div([
            html.Table(cc_divs)
        ])

        influencers_div = html.Div([
            html.Div(id='bc_table',children=bc_table,  style={'float':'left', 'padding':'1px', 'border':'1px black solid'}),
            html.Div(id='ec_table',children=ec_table,  style={'float':'left', 'padding':'1px', 'border':'1px black solid'}),
            html.Div(id='dc_table',children=dc_table,  style={'float':'left', 'padding':'1px', 'border':'1px black solid'}),
            html.Div(id='cc_table',children=cc_table,  style={'float':'left', 'padding':'1px', 'border':'1px black solid'}),
        ])
        return influencers_div

    # GENDER ANALYSIS
    def get_genderplot(self):
        if "genderplot" in self.dashboard_data:
            genders = pickle.loads(self.dashboard_data["genderplot"])
        else:
            genders = self.data_service.get_gender_percentages()
            self.dashboard_data["genderplot"] = pickle.dumps(genders)
            self.data_added = True
        
        fig = px.bar(genders, x="gender", y="num")
        return fig
        
    def get_gendertable(self):
        if "male_events" in self.dashboard_data and "female_events" in self.dashboard_data:
            female_events = pickle.loads(self.dashboard_data["female_events"])
            male_events = pickle.loads(self.dashboard_data["male_events"])
        else:
            female_events = self.data_service.get_events_gender(gender="female")
            male_events = self.data_service.get_events_gender(gender="male")
            self.dashboard_data["female_events"] = pickle.dumps(female_events)
            self.dashboard_data["male_events"] = pickle.dumps(male_events)
            self.data_added = True

        females = [html.Tr([html.Th("Event"), html.Th("Female participation rate")])]
        for index,i in female_events.iterrows():
            row = html.Tr([html.Td(html.A(href=i["event_url"], children=i["event"])),  html.Td(round(i["female"],6))])
            females.append(row)
        
        females_table = html.Div([
            html.Table(females)
        ])

        males = [html.Tr([html.Th("Event"), html.Th("Male participation rate")])]
        for index,i in male_events.iterrows():
            row = html.Tr([html.Td(html.A(href=i["event_url"], children=i["event"])), html.Td(round(i["male"],6))])
            males.append(row)
        
        males_table = html.Div([
            html.Table(males)
        ])

        gender_div = html.Div([
            html.Div(id='male_events',children=males_table,  style={'float':'left', 'padding':'1px'}),
            html.Div(id='female_events',children=females_table,  style={'float':'left', 'padding':'1px'})
        ])

        return gender_div
    
    # TOP GROUPS/LOCATIONS
    def get_topstable(self, limit=3):
        if "top_values" in self.dashboard_data:
            top_values = self.dashboard_data["top_values"]
            top_groups_comments = pickle.loads(top_values["top_groups_comments"])
            top_groups_people = pickle.loads(top_values["top_groups_people"])
            top_groups_event = pickle.loads(top_values["top_groups_event"])
            top_start_hours = pickle.loads(top_values["top_start_hours"])
            top_timeframes = pickle.loads(top_values["top_timeframes"])
        else:
            top_groups_comments = self.data_service.top_groups_comments(limit=limit)
            top_groups_people = self.data_service.top_groups_people(limit=limit)
            top_groups_event = self.data_service.top_groups(limit=limit)

            top_start_hours = self.data_service.top_start_hours(limit=limit)
            top_timeframes = self.data_service.top_timeframes(limit=limit)

            top_values_obj = {
                "top_groups_comments": pickle.dumps(top_groups_comments),
                "top_groups_people": pickle.dumps(top_groups_people),
                "top_groups_event": pickle.dumps(top_groups_event),
                "top_timeframes": pickle.dumps(top_timeframes),
                "top_start_hours": pickle.dumps(top_start_hours)
            }
            self.dashboard_data["top_values"] = top_values_obj
            self.data_added = True
        
        # top time frames and start hours
        timeframes = [html.Tr([html.Th("Duration"), html.Th("Counts")])]
        for index,i in top_timeframes.iterrows():
            row = html.Tr([html.Td(str(i["event_duration"])), html.Td(i["count"]) ])
            timeframes.append(row)
        
        hours = [html.Tr([html.Th("Start hour"), html.Th("Counts")])]
        timeframes+=hours
        for index,i in top_start_hours.iterrows():
            row = html.Tr([html.Td(str(i["event_time"])), html.Td(i["count"]) ])
            timeframes.append(row)

        # top groups by events, people and comments
        # by comments
        groups = [html.Tr([html.Th("Group"), html.Th("Comments")])]
        for index,i in top_groups_comments.iterrows():
            row = html.Tr([html.Td(str(i["group_name"])), html.Td(i["num_comments"]) ])
            groups.append(row)
        # by active users
        people = [html.Tr([html.Th("Group"), html.Th("Active users")])]
        groups += people
        for index,i in top_groups_people.iterrows():
            row = html.Tr([html.Td(str(i["group_name"])), html.Td(i["num_attendees"]) ])
            groups.append(row)
        # by events
        events = [html.Tr([html.Th("Group"), html.Th("Num events")])]
        groups += events
        for index,i in top_groups_event.iterrows():
            row = html.Tr([html.Td(str(i["index"])), html.Td(i["group_name"]) ])
            groups.append(row)

        tables = html.Div([
            html.Div([
                html.Table(id="timeframes_table",children=timeframes, style={'float':'left', 'padding':'1px'})
            ]),
            html.Div([
                html.Table(id="groups_table",children=groups, style={ 'float':'left', 'padding':'1px'})
            ])
            
        ])
        return tables

    

