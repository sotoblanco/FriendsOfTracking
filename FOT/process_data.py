from socceraction.data.wyscout import WyscoutLoader
from socceraction.spadl.wyscout import convert_to_actions
import socceraction.spadl as spadl
import socceraction.atomic.spadl as atomicspadl
import socceraction.vaep.features as features
import socceraction.vaep.labels as labels

import pandas as pd
import os

class DataProcessor:
    def __init__(self, competition, season, number_games):
        self.competition = competition
        self.season = season
        self.api = None
        self.last_games = None
        self.number_games = number_games
        
        self.functions_features = [
            features.actiontype_onehot,
            features.bodypart_onehot,
            features.result_onehot,
            features.goalscore,
            features.startlocation,
            features.endlocation,
            features.movement,
            features.space_delta,
            features.startpolar,
            features.endpolar,
            features.team,
            features.time_delta
        ]
        self.functions_labels = [
            labels.scores,
            labels.concedes
        ]
        self.columns_labels = [
            'scores',
            'concedes'
        ]
        self.columns_features = features.feature_column_names(self.functions_features, nb_prev_actions=3)
        self.dfs_features = []
        self.dfs_labels = []
        self.df_labels = None
        self.df_features = None

    def load_environment_vars(self):
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

    def authenticate(self):
        # set authentication credentials as environment variables
        username, password = self.load_environment_vars()

        # or provide authentication credentials as a dictionary
        self.api = WyscoutLoader(getter="remote", creds={"user": username, "passwd": password})

    def get_last_games(self):
        # get all matches from competition and season
        self.last_games = self.api.games(self.competition, self.season)
        self.last_games = self.last_games.head(self.number_games)
        self.last_games.to_hdf('FOT/data/last_games.h5', key='games')
        self.last_games.to_csv('FOT/data/last_games.csv', index=False)

    def process_games(self):

        for game_id, home_team_id in zip(self.last_games['game_id'].values, self.last_games['home_team_id'].values):
            last_game_event = self.api.events(game_id)
            spadl_df = convert_to_actions(last_game_event, home_team_id)
            team_name = self.api.teams(game_id=game_id)
            team_name.to_hdf('FOT/data/teams.h5', key=f'game_{game_id}')

            player_name = self.api.players(game_id=game_id)
            player_name.to_hdf('FOT/data/players.h5', key=f'game_{game_id}')

            df_actions = (
                spadl
                .add_names(spadl_df)  # add actiontype and result names
                .merge(team_name)  # add team names
                .merge(player_name)  # add player names
            )
            df_actions.to_hdf('FOT/data/actions.h5', key=f'game_{game_id}')

            df_actions_atomic = (
                atomicspadl
                .add_names(spadl_df)  # add actiontype and result names
                .merge(team_name)  # add team names
                .merge(player_name)  # add player names
            )

            df_actions_atomic.to_hdf('FOT/data/atomic_actions.h5', key=f'game_{game_id}')

            gamestates = features.gamestates(df_actions, 3)
            gamestates = features.play_left_to_right(gamestates, home_team_id)

            df_features = pd.concat([fn(gamestates) for fn in self.functions_features], axis=1)
            self.dfs_features.append(df_features[self.columns_features])

            df_features.to_hdf('FOT/data/features.h5', key=f'game_{game_id}')
            # 3. compute labels
            df_labels = pd.concat([fn(df_actions) for fn in self.functions_labels], axis=1)
            self.dfs_labels.append(df_labels[self.columns_labels])
            df_labels.to_hdf('FOT/data/labels.h5', key=f'game_{game_id}')

        df_labels = pd.concat(self.dfs_labels).reset_index(drop=True)
        df_features = pd.concat(self.dfs_features).reset_index(drop=True)
        # store the labels and features
        df_labels.to_hdf('FOT/data/all_labels.h5', key='labels')
        df_features.to_hdf('FOT/data/all_features.h5', key='features')



# Usage example
competition = 364
season = 188989
number_games = 2

processor = DataProcessor(competition, season, number_games)
processor.authenticate()
processor.get_last_games()
processor.process_games()
