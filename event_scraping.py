
from bs4 import BeautifulSoup
import requests
import sqlite3
import json
import pandas as pd

agentIds = ['Breach','Raze','Cypher','Sova','Killjoy','Viper','Phoenix',
            'Brimstone','Sage','Reyna','Omen','Jett','Skye','Yoru','Astra',
            'Kayo','Chamber','Neon','Fade']

def getAgentName(agentId):
    agentId = agentId - 1
    if agentId >= 0:
        return agentIds[agentId]
    else:
        return None

class Player:
    def __init__(self,playerId,name,agentId,teamName):
        self.id = playerId
        self.name = name
        self.agentId = agentId
        self.team = teamName
        self.agent_name = getAgentName(agentId)
        
        
def playerVars(playerData,seriesInfo):
    allPlayerInfo = {}
    for i,player in enumerate(playerData):
        team1 = seriesInfo['team1Name']
        team2 = seriesInfo['team2Name']
        if player['teamNumber'] == 2:
            teamName = team2
        if player['teamNumber'] == 1:
            teamName = team1   
            
        allPlayerInfo["P{0}".format(i+1)] = Player(player['playerId'],
                                                 player['player']['ign'],
                                                 player['agentId'],
                                                 teamName)
    return allPlayerInfo

def playersToDict(players):
    pD = {}
    for i,p in enumerate(players):
        playerNS = 'player' + str(i+1)
        playerName = playerNS + 'Name'
        playerId = playerNS + 'Id'
        playerAgent = playerNS + 'AgentName'
        playerTeam = playerNS + 'Team'
        
        
        pD[playerId] = p.id
        pD[playerName] = p.name
        pD[playerAgent] = p.agent_name
        pD[playerTeam] = p.team
    return pD
        

def getJson(url):
    match_page = requests.get(url)
    match_json = BeautifulSoup(match_page.text, features='lxml')
    match_json = match_json.get_text()
    match_json = json.loads(match_json)
    return match_json


rib_bapi = 'https://backend-prod.rib.gg/v1/events/'
series_bapi = 'https://backend-prod.rib.gg/v1/series/'
match_data_api = 'https://backend-prod.rib.gg/v1/matches/'


json_page = rib_bapi + '1199' # Put the eventID you want to scrape here


event = getJson(json_page)
series_list = event['series']

conn = sqlite3.connect('NAStage1Challengers.db') # name of database you want to save to
cursor = conn.cursor()

map_index = 0
round_index = 0
match_index = 0


series_added = 0
full_series_data = pd.DataFrame()
full_xvy_data = pd.DataFrame()
full_match_data = pd.DataFrame()
full_player_data = pd.DataFrame()
full_round_data = pd.DataFrame()
full_kill_data = pd.DataFrame()
full_playerStats_data = pd.DataFrame()
full_match_details = pd.DataFrame()
full_match_locations = pd.DataFrame()


for s in series_list:

    
    
    sId = str(s['id'])
    series_page = series_bapi + sId
    series_json = getJson(series_page)

    series_data = {}
    series_data['seriesId'] = series_json['id']
    series_data['eventId'] = series_json['eventId']
    series_data['eventName'] = series_json['eventName']
    series_data['parentEventId'] = series_json['parentEventName']
    series_data['parentEventName'] = series_json['parentEventId']
    series_data['team1Id'] = series_json['team1Id']
    series_data['team1Name'] = series_json['team1']['name']
    series_data['team1Region'] = series_json['team1']['regionId']
    series_data['team2Id'] = series_json['team2Id']
    series_data['team2Name'] = series_json['team2']['name']
    series_data['team2Region'] = series_json['team2']['regionId']
    series_data['date'] = series_json['startDate']
    series_data['bestOf'] = series_json['bestOf']
    series_to_add = pd.DataFrame(series_data, index=[0])
    full_series_data = pd.concat([full_series_data, series_to_add])
    
    
    for match in series_json['matches']:
        if match['completed'] is True:
            match_data = {}
            match_data['matchId'] = match['id']
            match_data['seriesId'] = match['seriesId']
            match_data['map'] = match['map']['name']
            match_data['winningTeamNumber'] = match['winningTeamNumber']
            match_data['team1Score'] = match['team1Score']
            match_data['team2Score'] = match['team2Score']
            
            matchDetailsJson = getJson(match_data_api+str(match['id'])+'/details')
            matchDetailsEvents = matchDetailsJson['events']
            matchDetailsLoc = matchDetailsJson['locations']
            
            match_details_add = pd.DataFrame.from_dict(matchDetailsEvents)
            full_match_details = pd.concat([full_match_details,match_details_add])
            
            locations_add = pd.DataFrame.from_dict(matchDetailsLoc)
            full_match_locations = pd.concat([full_match_locations,locations_add])
            
            match_data_add = pd.DataFrame(match_data,index=[0])
            full_match_data = pd.concat([full_match_data,match_data_add])
            
            player_ids = {}
            players = playerVars(match['players'],series_data)
            players = list(players.values())
            player_ids = playersToDict(players)
            player_ids['matchId'] = match['id']
            p_add = pd.DataFrame(player_ids, index=[0])
            full_player_data = pd.concat([full_player_data, p_add])
    round_data = pd.DataFrame.from_dict(series_json['stats']['rounds'])
    full_round_data = pd.concat([full_round_data,round_data])
    kill_data = pd.DataFrame.from_dict(series_json['stats']['kills'])
    full_kill_data = pd.concat([full_kill_data,kill_data])
    xvy_data = pd.DataFrame.from_dict(series_json['stats']['xvys'])
    full_xvy_data = pd.concat([full_xvy_data,xvy_data])
    ps_data = pd.DataFrame.from_dict(series_json['playerStats'])
    full_playerStats_data = pd.concat([full_playerStats_data,ps_data])
    series_added += 1
    print('series #',series_added,' has been fully logged')
        
    
full_kill_data.drop(columns=['assistants'],inplace=True)
full_match_details.drop(columns=['assists'],inplace=True)


full_series_data.to_sql('series', conn, if_exists='append')
full_xvy_data.to_sql('xvy', conn, if_exists='append')
full_match_data.to_sql('matches', conn, if_exists='append')
full_player_data.to_sql('players', conn, if_exists='append')
full_round_data.to_sql('rounds', conn, if_exists='append')
full_kill_data.to_sql('kills', conn, if_exists='append')
full_playerStats_data.to_sql('pStats', conn, if_exists='append')
full_match_locations.to_sql('locations',conn,if_exists='append')
full_match_details.to_sql('details',conn,if_exists='append')

conn.close()