from xgboost import XGBClassifier
import pandas as pd

columns_labels = [
    'scores',
    'concedes'
]
df_games = pd.read_csv('FOT/data/last_games.csv')
df_labels = pd.read_hdf('FOT/data/all_labels.h5', key='labels')
df_features = pd.read_hdf('FOT/data/all_features.h5', key='features')

models = {}
for column_labels in columns_labels:
    print(column_labels)
    model = XGBClassifier(
        eval_metric='logloss',
        use_label_encoder=False
    )
    model.fit(df_features, df_labels[column_labels])
    models[column_labels] = model

dfs_predictions = {}
for column_labels in columns_labels:
    model = models[column_labels]
    probabilities = model.predict_proba(df_features)
    predictions = probabilities[:, 1]
    dfs_predictions[column_labels] = pd.Series(predictions)
df_predictions = pd.concat(dfs_predictions, axis=1)

dfs_game_ids = []
for game_id, home_team_id in zip(df_games['game_id'].values, df_games['home_team_id'].values):
    df_actions = pd.read_hdf('FOT/data/actions.h5', key=f'game_{game_id}')
    dfs_game_ids.append(df_actions['game_id'])
df_game_ids = pd.concat(dfs_game_ids, axis=0).astype('int').reset_index(drop=True)

df_predictions = pd.concat([df_predictions, df_game_ids], axis=1)
df_predictions_per_game = df_predictions.groupby('game_id')

for game_id, df_predictions in df_predictions_per_game:
    df_predictions = df_predictions.reset_index(drop=True)
    df_predictions[columns_labels].to_hdf('FOT/data/predictions.h5', key=f'game_{game_id}')