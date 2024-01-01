import os
import wyscoutapi
import pandas as pd

def load_environment_vars():
    # Get the current working directory
    cwd = os.getcwd()

    # Append the .env file name
    env_path = os.path.join(cwd, '.env')

    # Open the .env file
    with open(env_path) as f:
        # Read the lines
        lines = f.readlines()

    # Parse the key-value pairs
    for line in lines:
        # Ignore comments and empty lines
        if line.startswith('#') or not line.strip():
            continue
        # Split the line into key and value
        key, value = line.strip().split('=', 1)
        # Set the environment variable
        os.environ[key] = value

    return os.environ['WYSCOUT_API_USER'], os.environ['WYSCOUT_API_PASSWORD']

def create_wyscout_client(username, password):
    return wyscoutapi.WyscoutAPI(username=username, password=password)

def get_teams_and_seasons(csv_path, num_seasons=4):
    """
    Retrieves teams and seasons data from Wyscout API using the provided CSV file path.

    Args:
        csv_path (str): The file path of the CSV file containing Wyscout IDs of the teams.
        num_seasons (int, optional): The number of seasons to retrieve. Defaults to 4.

    Returns:
        pandas.DataFrame: A DataFrame containing the teams and their respective seasons.
    """
    username, password = load_environment_vars()
    client = create_wyscout_client(username, password)

    teams_df = pd.read_csv(csv_path)

    leagues = teams_df['Wyscout ID'].unique()

    league_df = pd.DataFrame()
    season_dict = {}
    season_list = []
    for league in leagues:
        print(f'Getting teams and seasons for league {league}')
        league_list = pd.DataFrame(client.competition_teams(league)['teams'])[['wyId', 'name', 'officialName']]
        for i in range(1, num_seasons + 1):
            season_values = client.competition_seasons(competition_id=league)['seasons'][i]['season']['wyId']
            season_list.append(season_values)
            season_dict[league] = season_values
            league_list[f'season_p{i}'] = season_values
        league_list['league_id'] = league
        league_df = pd.concat([league_df, league_list], ignore_index=True)

    transfermark_dict = dict(zip(teams_df['Wyscout ID'], teams_df['transfermarkt id']))
    league_name_dict = dict(zip(teams_df['Wyscout ID'], teams_df['League Name']))

    league_df['transfermarkt_id'] = league_df['league_id'].map(transfermark_dict)
    league_df['league_name'] = league_df['league_id'].map(league_name_dict)

    return league_df

if __name__ == '__main__':
    teams_df = get_teams_and_seasons('data/raw/league_id2.csv', num_seasons=4)
    teams_df.to_csv('data/raw/teams_and_seasons.csv', index=False)
    print(teams_df.head())