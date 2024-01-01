import requests
import pandas as pd

# Constants
BASE_URL = "https://transfermarkt-api.vercel.app"
YEAR = 2023

def fetch_json(url):
    """ Fetch JSON data from a given URL """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    return None

def get_team_ids(league_id):
    """ Get team IDs for a given league """
    url = f"{BASE_URL}/competitions/{league_id}/clubs?season_id={YEAR}"
    data = fetch_json(url)
    return data['clubs'] if data else None

def get_team_value(team_id):
    """ Get market value for a given team """
    url = f"{BASE_URL}/clubs/{team_id}/profile"
    data = fetch_json(url)
    return data['currentMarketValue'] if data else None

def convert_value_to_float(value):
    """ Convert a string value to float considering its unit """
    if not value:
        return None
    elif 'k' in value:
        return float(value.strip('€').strip('k')) * 1
    elif 'm' in value:
        return float(value.strip('€').strip('m')) * 1000
    elif 'bn' in value:
        return float(value.strip('€').strip('bn')) * 1000000
    return float(value.strip('€'))

def process_leagues(league_ids):
    """ Process leagues to extract team market values """
    value_df = pd.DataFrame()
    for league_id in league_ids:
        print(f"Processing league: {league_id}")
        teams = get_team_ids(league_id)
        if not teams:
            print('No teams found')
            continue

        for team in teams:
            try:
                market_value = get_team_value(team['id'])
            except:
                market_value = None
            value_row = pd.DataFrame([{
                'team_id': team['id'],
                'name_id': team['name'],
                'league_id': league_id,
                'market_value': market_value
            }])
            value_df = pd.concat([value_df, value_row], ignore_index=True)

    return value_df

# Main process
teams_df = pd.read_csv('data/raw/league_id2.csv')
league_ids = teams_df['transfermarkt id'].unique()

market_values_df = process_leagues(league_ids)
market_values_df['team_value'] = market_values_df['market_value'].apply(convert_value_to_float)
market_values_df.drop(columns=['market_value'], inplace=True)

league_markt_value = market_values_df.groupby('league_id').agg({'team_value': 'sum'}).reset_index()
league_dict_value = dict(zip(league_markt_value['league_id'], league_markt_value['team_value']))
market_values_df['league_value'] = market_values_df['league_id'].map(league_dict_value)

print(market_values_df.head)
market_values_df.to_csv('data/raw/team_market_value2.csv', index=False)
