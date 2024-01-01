import pandas as pd

from get_stats import get_wyscout_client


def get_points_season(season_id_list, client):
    team_points = {}
    for season_id in season_id_list:
        print(f'Getting points for season {season_id}')
        try:
            league_team_result = client.season_career(season_id)['rounds'][0]['groups'][0]['teams']
        except:
            print(f'Error in season {season_id}')
            continue
        for j in range(len(league_team_result)):
            try:
                team_points[league_team_result[j]['teamId']] = league_team_result[j]['points']
            except:
                print(f'Error in season {season_id}, team {league_team_result[j]["teamId"]}')
                continue
    return team_points

if __name__ == '__main__':

    client = get_wyscout_client()
    csv_path = 'data/raw/teams_and_seasons.csv'
    df = pd.read_csv(csv_path)

    season_list = ["p1", "p2", "p3", "p4"]
    
    for season in season_list:
        unique_season_id = df[f'season_{season}'].unique()
        points_dict = get_points_season(unique_season_id, client)
        df[f'team_points_{season}'] = df['wyId'].map(points_dict)
    print(df.head())
    df.to_csv('data/raw/teams_and_seasons.csv', index=False)

