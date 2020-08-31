from pymongo import MongoClient
from dash.dependencies import Input, Output, State
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go 
import numpy as np 
import requests, json
import pandas as pd
import plotly.express as px

import meetup_graph as service
import plotly.graph_objects as go

import time

start_time = time.time()
    
meeteup = service.MeetupGraph("Paris", "french")


# heatmap container
heatmap_fig = meeteup.get_heatmap()
heatmap_graph = html.Div([
    dcc.Graph(figure=heatmap_fig)
])

# events map container
eventsmap_fig = meeteup.get_eventsmap()
eventsmap_graph = html.Div([
    dcc.Graph(figure=eventsmap_fig)
])

# network graph
network_fig = meeteup.get_network()
network_graph = html.Div([
    dcc.Graph(figure=network_fig)
])

# wordcloud image
wordcloud_fig = meeteup.get_wordcloud()
wordcloud_graph = html.Div([
    html.Img(src=wordcloud_fig)
])

# general stats
general_stats_table = meeteup.get_generalstats()
general_stats = html.Div(children=general_stats_table)

# top influencers
influencers_tables=meeteup.get_influencers()
# events w gender table
# gender bar plot
gender_fig = meeteup.get_genderplot()
gender_graph = html.Div([
    dcc.Graph(figure=gender_fig)
])
# gender table
gendertable = meeteup.get_gendertable()
# tops table
topstable = meeteup.get_topstable()
app = dash.Dash()

meeteup.save_dashboard_data()
app.layout = html.Div([
html.Div(id='heatmap',children=heatmap_graph,  style={'width':'650px', 'float':'left', 'margin':'1px', 'border':'1px black solid'}),
html.Div(id='eventsmap',children=eventsmap_graph,  style={'width':'950px', 'float':'left','margin':'1px', 'border':'1px black solid'}),
html.Div(id='network',children=network_graph,  style={'width':'950px', 'float':'left','margin':'1px', 'border':'1px black solid'}),
html.Div(id='wordcloud',children=wordcloud_graph,  style={'width':'650px','float':'left', 'margin':'1px', 'border':'1px black solid'}),
html.Div(id='generalstats',children=general_stats,  style={'width':'300px','float':'left', 'margin':'1px', 'border':'1px black solid'}),
html.Div(id='influencers',children=influencers_tables,  style={'width':'700px','float':'left', 'margin':'1px', 'border':'1px black solid'}),
html.Div(id='genderplot',children=gender_graph,  style={'width':'600px','float':'left', 'margin':'1px', 'border':'1px black solid'}),
html.Div(id='gendertable',children=gendertable,  style={'width':'750px','float':'left', 'margin':'1px', 'border':'1px black solid'}),
html.Div(id='topstable',children=topstable,  style={'width':'600px','float':'left', 'margin':'1px', 'border':'1px black solid'})


])


print("--- %s seconds ---" % (time.time() - start_time))

app.run_server(debug=True, use_reloader=False) 