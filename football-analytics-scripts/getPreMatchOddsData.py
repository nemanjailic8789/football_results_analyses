import requests
import pandas as pd
import os
from dotenv import load_dotenv

#load environment variables from .env file
load_dotenv()
api_token = os.getenv("api_token")
fixtures_endpoint = os.getenv("fixtures_endpoint")
odds_endpoint = os.getenv("odds_endpoint")
rapid_api_host = os.getenv("rapid_api_host")
bundesliga_id = os.getenv("bundesliga_id")

headers = {
    "X-RapidAPI-Key": api_token,
    "X-RapidAPI-Host": rapid_api_host
}

def get_data(endpoint, query):
    response = requests.get(endpoint, params=query, headers=headers)
    return response

#get fixture_id by round
query = {"league": bundesliga_id, "season":"2023", "round": 'Regular Season - 12'}
response = get_data(fixtures_endpoint, query)
response = response.json()

fixture_id_list = [fixture['fixture']['id'] for fixture in response['response']]
fixture_dt_list = [fixture['fixture']['date'] for fixture in response['response']]
home_team_id_list = [fixture['teams']['home']['id'] for fixture in response['response']]
home_team_name_list = [fixture['teams']['home']['name'] for fixture in response['response']]
away_team_id_list = [fixture['teams']['away']['id'] for fixture in response['response']]
away_team_name_list = [fixture['teams']['away']['name'] for fixture in response['response']]

data_to_write = {
    "fixture_id": fixture_id_list,
    "fixture_dt": fixture_dt_list,
    "home_team_id": home_team_id_list,
    "home_team_name": home_team_name_list,
    "away_team_id": away_team_id_list,
    "away_team_name": away_team_name_list
}

#get pre-match odds by fixture_id
home_win_odds_list, draw_odds_list, away_win_odds_list = [[],[],[]]
goals_over_2_5_odds_list, goals_under_2_5_odds_list, goals_over_3_5_odds_list, goals_under_3_5_odds_list, goals_over_1_5_odds_list, goals_under_1_5_odds_list = [[],[],[],[],[],[]]
both_teams_score_yes_odds_list, both_teams_score_no_odds_list = [[],[]]
for fixture_id in fixture_id_list:
    print("Getting data from fixture: ", fixture_id)
    query = {"fixture": fixture_id}
    response = get_data(odds_endpoint, query)
    response = response.json()
    
    bet365Odds = [bookmaker for bookmaker in response['response'][0]['bookmakers'] if bookmaker['name'] == "Bet365"]
    
    match_winner_object = list(filter(lambda x: x['name'] == 'Match Winner', bet365Odds[0]['bets']))
    if match_winner_object:
        home_win_odds = [odd['odd'] for odd in match_winner_object[0]['values'] if odd['value'] == 'Home']
        home_win_odds_list.append(home_win_odds[0])
        draw_odds = [odd['odd'] for odd in match_winner_object[0]['values'] if odd['value'] == 'Draw']
        draw_odds_list.append(draw_odds[0])
        away_win_odds = [odd['odd'] for odd in match_winner_object[0]['values'] if odd['value'] == 'Away']
        away_win_odds_list.append(away_win_odds[0])
    else:
        print("No match winner odds")
    
    goals_over_under_object = list(filter(lambda x: x['name'] == 'Goals Over/Under', bet365Odds[0]['bets']))
    if goals_over_under_object:
        goals_over_2_5_odds = [odd['odd'] for odd in goals_over_under_object[0]['values'] if odd['value'] == 'Over 2.5']
        goals_over_2_5_odds_list.append(goals_over_2_5_odds[0])
        goals_under_2_5_odds = [odd['odd'] for odd in goals_over_under_object[0]['values'] if odd['value'] == 'Under 2.5']
        goals_under_2_5_odds_list.append(goals_under_2_5_odds[0])
        goals_over_3_5_odds = [odd['odd'] for odd in goals_over_under_object[0]['values'] if odd['value'] == 'Over 3.5']
        goals_over_3_5_odds_list.append(goals_over_3_5_odds[0])
        goals_under_3_5_odds = [odd['odd'] for odd in goals_over_under_object[0]['values'] if odd['value'] == 'Under 3.5']
        goals_under_3_5_odds_list.append(goals_under_3_5_odds[0])
        goals_over_1_5_odds = [odd['odd'] for odd in goals_over_under_object[0]['values'] if odd['value'] == 'Over 1.5']
        goals_over_1_5_odds_list.append(goals_over_1_5_odds[0])
        goals_under_1_5_odds = [odd['odd'] for odd in goals_over_under_object[0]['values'] if odd['value'] == 'Under 1.5']
        goals_under_1_5_odds_list.append(goals_under_1_5_odds[0])
    else:
        print("No goals over/under odds")
        
    both_teams_score_obj = list(filter(lambda x: x['name'] == 'Both Teams Score', bet365Odds[0]['bets']))
    if both_teams_score_obj:
        both_teams_score_yes_odds = [odd['odd'] for odd in both_teams_score_obj[0]['values'] if odd['value'] == 'Yes']
        both_teams_score_yes_odds_list.append(both_teams_score_yes_odds[0])
        both_teams_score_no_odds = [odd['odd'] for odd in both_teams_score_obj[0]['values'] if odd['value'] == 'No']
        both_teams_score_no_odds_list.append(both_teams_score_no_odds[0])
    else:
        print("No both teams to score odds")

data_to_write['home_win_odds'] = home_win_odds_list
data_to_write['draw_odds'] = draw_odds_list
data_to_write['away_win_odds'] = away_win_odds_list
data_to_write['goals_over_2.5_odds'] = goals_over_2_5_odds_list
data_to_write['goals_under_2.5_odds'] = goals_under_2_5_odds_list
data_to_write['goals_over_3.5_odds'] = goals_over_3_5_odds_list
data_to_write['goals_under_3.5_odds'] = goals_under_3_5_odds_list
data_to_write['goals_over_1.5_odds'] = goals_over_1_5_odds_list
data_to_write['goals_under_1.5_odds'] = goals_under_1_5_odds_list
data_to_write['both_teams_score_yes_odds'] = both_teams_score_yes_odds_list
data_to_write['both_teams_score_no_odds'] = both_teams_score_no_odds_list

temp_df = pd.DataFrame(data_to_write)
result_df = pd.read_csv('result_df.csv')
#temp_df.to_csv("Round 11 data.csv", index=False)

#check if both dataframes have the same columns and in the same order and append new data if the check is ok
if list(result_df.columns) == list(temp_df.columns):
    result_df = pd.concat([result_df, temp_df], ignore_index=True)
    result_df.to_csv('result_df.csv', index=False)
    print("DataFrames have the same columns. New data appended.")
else:
    print("DataFrames have different columns.")