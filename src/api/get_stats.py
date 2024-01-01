import pandas as pd
from wyscout import load_environment_vars, create_wyscout_client

def get_wyscout_client():
    """ Load environment variables and create a Wyscout client """
    username, password = load_environment_vars()
    return create_wyscout_client(username, password)

def fetch_team_stats(client, team_id, competition_id, season_id, type='total'):
    """ Fetch advanced stats for a given team, competition, and season. 
    type can be: total, average, percentage"""
    try:
        stats = client.team_advancedstats(team_id=team_id, 
                                          competition_id=competition_id,
                                          season_id=season_id)

        return stats[type] if stats else None
    except Exception as e:
        print(f"Error fetching stats for team {team_id}, season {season_id}: {e}")
        return None

def process_leagues_for_season(league_df, season_column, type):
    """ Process leagues to extract team stats for a specified season column """
    stats_df = pd.DataFrame()
    for _, row in league_df.iterrows():
        season_id = row[season_column]
        if pd.notna(season_id):
            print(f"Processing {row['officialName']} for {season_column}: {season_id}")
            stats = fetch_team_stats(client, row['wyId'], row['league_id'], season_id, type)
            if stats:
                stats_row = pd.DataFrame([stats])
                stats_row['team_id'] = row['wyId']
                stats_row['season_id'] = season_id
                stats_row['competition_id'] = row['league_id']
                stats_row['officialName'] = row['officialName']
                stats_row['name'] = row['name']
                stats_row['league_name'] = row['league_name']
                stats_row['transfermarkt_id'] = row['transfermarkt_id']
                stats_df = pd.concat([stats_df, stats_row])
        else:
            print(f"Skipping {row['officialName']} for {season_column}: No season ID")

    return stats_df

# Main process
if __name__ == '__main__':

    client = get_wyscout_client()
    league_df = pd.read_csv('data/raw/teams_and_seasons.csv')

    # Example: Process for a specific season column
    season_column = ['season_p1', 'season_p2', 'season_p3', 'season_p4']  # Can be changed to season_p2, season_p3, etc.
    for season in season_column:
        stats_df = process_leagues_for_season(league_df, season, type='total')
        print(stats_df.head())
        stats_df.to_csv(f'data/processed/processed_stats_{season}.csv', index=False)
    #stats_df = process_leagues_for_season(league_df, season_column, type='total')
    #print(stats_df.head())
    #stats_df.to_csv(f'data/processed/processed_stats_{season_column}.csv', index=False)
