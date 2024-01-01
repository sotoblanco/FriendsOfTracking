# run the process_data.py script to get the process_data
# run the train_classifier.py script to get the train_classifier
# run the vaep.py to get the value of the player in the selected timeframes

from process_data import DataProcessor

# Usage example
competition = 364
season = 188989
number_games = 2

processor = DataProcessor(competition, season, number_games)
processor.authenticate()
processor.get_last_games()
processor.process_games()

