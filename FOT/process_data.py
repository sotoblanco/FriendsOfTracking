from socceraction.data.wyscout import WyscoutLoader
from socceraction.spadl.wyscout import convert_to_actions
import socceraction.spadl as spadl
import socceraction.atomic.spadl as atomicspadl
import socceraction.vaep.features as features
import socceraction.vaep.labels as labels

import pandas as pd
import os

class DataProcessor:
    def __init__(self, competition):
        self.competition = competition
        self.api = None
        
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

    def get_last_games(self, season):
        # get all matches from competition and season
        print(f'Getting last games for competition: {self.competition} and season: {season}')
        last_games = self.api.games(self.competition, season)
        #self.last_games = self.last_games.head(self.number_games)
        last_games.to_hdf('FOT/data/last_games.h5', key='games')
        last_games.to_csv(f'FOT/data/last_games_{self.competition}_{season}.csv', index=False)
        return last_games

    def get_feature_game(self, game_id, home_team_id):

        # get events
        last_game_event = self.api.events(game_id)
        spadl_df = convert_to_actions(last_game_event, home_team_id)
        
        df_actions = (spadl.add_names(spadl_df))  # add actiontype and result names
        
        df_actions.to_hdf('FOT/data/actions.h5', key=f'game_{game_id}')
                        
        gamestates = features.gamestates(df_actions,3)
        gamestates = features.play_left_to_right(gamestates, home_team_id)

        # 2. compute features
        df_features = pd.concat([fn(gamestates) for fn in self.functions_features], axis=1)
        df_features.to_hdf('FOT/data/features.h5', key=f'game_{game_id}')
        # 3. compute labels
        df_labels = pd.concat([fn(df_actions) for fn in self.functions_labels], axis=1)
        df_labels.to_hdf('FOT/data/labels.h5', key=f'game_{game_id}')

        # get teams
        team_name = self.api.teams(game_id=game_id)
        team_name.to_hdf('FOT/data/teams.h5', key=f'game_{game_id}')

        # get players
        player_name = self.api.players(game_id=game_id)
        player_name.to_hdf('FOT/data/players.h5', key=f'game_{game_id}')

    def process_features(self, games):
        for row in games.itertuples():
            game_id = row.game_id
            home_team_id = row.home_team_id
            print(f'Processing game {game_id}')
            self.get_feature_game(game_id, home_team_id)

    def get_competition(self, num_season):
        competitions = self.api.competitions(self.competition)
        return competitions.head(num_season)

def main():

    competitions = {'Premier League': 364,
                #'Ligue 1': 412,
                'Bundesliga': 426,
                'Serie A': 524, 
                'La Liga': 795}
    season_number = 3

    for competition, competition_id in competitions.items():
        processor = DataProcessor(competition_id)
        processor.authenticate()
        seasons = processor.get_competition(num_season=season_number)
        for season in seasons['season_id']: 
            print(f'Processing league: {competition} and season: {season}')
            try:
                games = processor.get_last_games(season=season)
            except KeyError:
                print('Season not found')
                continue
            processor.process_features(games=games)

if __name__ == "__main__":
    main()