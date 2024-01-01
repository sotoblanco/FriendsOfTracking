# VAEP
# This script computes the VAEP value for each player in the last games we selected
# ideas on how to use: the importance of a player for the team succed or failure
# For instance we can answer questions about how critical was Halland for the results of the Manchester City in the premier league this season
# Generate a rank of players by league

import socceraction.vaep.formula as vaepformula
import pandas as pd

minutes_game = 60
path = 'FOT/data/'

# 6. compute VAEP value
df_games = pd.read_csv(path + 'last_games.csv')

dfs_values = []
for game_id, home_team_id in zip(df_games['game_id'].values, df_games['home_team_id'].values):
    df_actions = pd.read_hdf(path + 'actions.h5', key=f'game_{game_id}')
    df_players = pd.read_hdf(path + 'players.h5', key=f'game_{game_id}')
    df_teams = pd.read_hdf(path + 'teams.h5', key=f'game_{game_id}')
    df_actions = (df_actions
        .merge(df_players, how='left')
        .merge(df_teams, how='left')
        .reset_index(drop=True)
    )
    
    df_predictions = pd.read_hdf(path + 'predictions.h5', key=f'game_{game_id}')
    df_values =  vaepformula.value(df_actions, df_predictions['scores'], df_predictions['concedes'])
    
    df_all = pd.concat([df_actions, df_predictions, df_values], axis=1)
    dfs_values.append(df_all)


df_values = (pd.concat(dfs_values)
    .sort_values(['game_id', 'period_id', 'time_seconds'])
    .reset_index(drop=True)
)

df_ranking = (df_values[['player_id', 'team_name', 'nickname', 'vaep_value']]
    .groupby(['player_id', 'team_name', 'nickname'])
    .agg(vaep_count=('vaep_value', 'count'), vaep_sum=('vaep_value', 'sum'))
    .sort_values('vaep_sum', ascending=False)
    .reset_index()
)

df_values.drop_duplicates(subset=['game_id', 'player_id', 'minutes_played'], keep='first', inplace=True)
df_minutes_played = df_values.groupby(['game_id', 'player_id'])['minutes_played'].sum().reset_index()

df_ranking_p90 = df_ranking.merge(df_minutes_played)
df_ranking_p90 = df_ranking_p90[df_ranking_p90['minutes_played'] > minutes_game]
df_ranking_p90['vaep_rating'] = df_ranking_p90['vaep_sum'] * 90 / df_ranking_p90['minutes_played']
df_ranking_p90 = df_ranking_p90.sort_values('vaep_rating', ascending=False)
df_ranking_p90.to_csv(path + 'ranking.csv', index=False)