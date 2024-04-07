import pandas as pd
import os
import socceraction.spadl as spadl
from socceraction.vaep.formula import value

home = "/home/pastor/projects/FriendsOfTracking/FOT/data"

# read all csv files in the folder and append in a single dataframe
def read_all_csv_files(folder):
    df = pd.DataFrame()
    for filename in os.listdir(folder):
        if filename.endswith('.csv'):
            df = pd.concat([df, pd.read_csv(os.path.join(folder, filename))])
    
    df.drop_duplicates(subset='game_id', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def get_features_labels(games):
    dfs_features = []
    dfs_labels = []
    for row in games.itertuples():
        game_id = row.game_id
        df_features = pd.read_hdf(home + 'features.h5', key=f'game_{game_id}')
        df_features['game_id'] = game_id
        dfs_features.append(df_features)

        df_labels = pd.read_hdf(home + 'labels.h5', key=f'game_{game_id}')
        df_labels['game_id'] = game_id
        dfs_labels.append(df_labels)
        
    df_labels = pd.concat(dfs_labels).reset_index(drop=True)
    df_features = pd.concat(dfs_features).reset_index(drop=True)

    return df_features, df_labels

def get_actions(games):
    
    dfs_actions = []
    for row in games.itertuples(): #zip(last_games['game_id'].values, last_games['home_team_id'].values):
        game_id = row.game_id
        
        #print(f'Processing game {game_id}')
        df_actions = pd.read_hdf(home + 'actions.h5', key=f'game_{game_id}')
        team_name = pd.read_hdf(home + 'teams.h5', key=f'game_{game_id}')
        player_name = pd.read_hdf(home + 'players.h5', key=f'game_{game_id}')
        df_actions = (spadl
            #df_actions#spadl
            .add_names(df_actions)  # add actiontype and result names
            .merge(team_name, how='left')  # add team names
            .merge(player_name, how='left')  # add player names
            #.sort_values(["game_id", "period_id", "action_id"])
            .reset_index()
            .rename(columns={'index': 'action_id'})
        )
        dfs_actions.append(df_actions)
    df_actions = pd.concat(dfs_actions).reset_index(drop=True)

    return df_actions

df = read_all_csv_files(home)
df_features, df_labels = get_features_labels(df)
df_actions = get_actions(df)

from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

# find a better validation technique
features = [
    'start_dist_to_goal_a0',
    'end_dist_to_goal_a0',
    'start_dist_to_goal_a1',
    'end_dist_to_goal_a1',
    'start_dist_to_goal_a2',
    'end_dist_to_goal_a2',
    'start_angle_to_goal_a0',
    'end_angle_to_goal_a0',
    'start_angle_to_goal_a1',
    'end_angle_to_goal_a1',
    'start_angle_to_goal_a2',
    'end_angle_to_goal_a2',
    'team_1',
    'team_2'
]
labels = ['scores',
          'concedes'
          ]

df_X_train, df_X_test, df_y_train, df_y_test = train_test_split(
    df_features,
    df_labels,
    test_size=0.1,
    random_state=42,
    stratify=df_labels['scores'].astype(str) + '_' + df_labels['concedes'].astype(str)
)

models = {}
for label in labels:
    model = XGBClassifier(
        eval_metric='logloss',
        use_label_encoder=False,
        n_estimators=100,
        max_depth=3
    )
    model.fit(
        X=df_X_train[features],
        y=df_y_train[label]
    )
    models[label] = model

dfs_predictions = {}
for label in labels:
    model = models[label]
    probabilities = model.predict_proba(
        df_features[features]
    )
    predictions = probabilities[:, 1]
    dfs_predictions[label] = pd.Series(predictions, index=df_features.index)
df_predictions = pd.concat(dfs_predictions, axis=1)

df_predictions.to_csv(home + 'predictions.csv', index=False)

df_actions_predictions = pd.concat([df_actions, df_predictions], axis=1)

dfs_values = []
for game_id, game_predictions in df_actions_predictions.groupby('game_id'):
    df_values = value(game_predictions, game_predictions['scores'], game_predictions['concedes'])
    
    df_all = pd.concat([game_predictions, df_values], axis=1)
    dfs_values.append(df_all)

df_values = (pd.concat(dfs_values)
    .sort_values(['game_id', 'period_id', 'time_seconds'])
    .reset_index(drop=True)
)

df_ranking = (df_values[['player_id', 'team_name', 'nickname', 'vaep_value']]
    .groupby(['player_id', 'team_name', 'nickname'])
    .agg(vaep_count=('vaep_value', 'count'), 
         vaep_mean=('vaep_value', 'mean'),
         vaep_sum=('vaep_value', 'sum'))
    .sort_values('vaep_sum', ascending=False)
    .reset_index()
)

df_player_games = []
for row in df.itertuples():
    game_id = row.game_id
    game_player = pd.read_hdf('players.h5', key=f'game_{game_id}')
    df_player_games.append(game_player)

df_player_games = pd.concat(df_player_games).reset_index(drop=True)
df_minutes_played = df_player_games.groupby(['player_id'])['minutes_played'].sum().reset_index()
df_ranking_p90 = df_ranking.merge(df_minutes_played)
df_ranking_p90['vaep_rating'] = df_ranking_p90['vaep_sum'] * 90 / df_ranking_p90['minutes_played']
df_ranking_p90['actions_p90'] = df_ranking_p90['vaep_count'] * 90 / df_ranking_p90['minutes_played']