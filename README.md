# Football project

The overall pipeline is as follows:

1. We have a baseline file that contains the Wyscout ID for the league and the transfermarkt ID for the league. We use this to get the Wyscout ID for each team in the league. We have 58 leagues.

2. With the Wyscout ID for each league we can get the Wyscout ID for each team, the name and the official name. We also append the league id to each team. 

3. At this point we have a file with the Wyscout ID for each team, the name and the official name, the league ID and the number of seasons we selected.

![Alt text](image.png) 

4. Usint the ``transfermark.py`` file we can get the transfermarkt ID for each team. We also append the transfermarkt ID to each team and the market value. 

5. ``get_stats.py`` it is used to obtain the stats for each team, it requires the wyscout ID, the competition id and the season id. It returns a file with the stats for each team.


## Team statistics

This process takes raw inputs from a definition of leagues that we want and returns a file with stats of a season, market value, league value and points on that season. 

1. ``wyscout.py`` : Takes the initial raw file ``league_id2.csv`` with the leagues ``Wyscout ID``, and the ``transfermarkt_id`` and returns a file with the ``Wyscout ID`` for each team, the name and the official name, the league ID and the number of seasons we selected in the ``teams_and_seasons.csv`` file. This provide the id needed for each team, league and season (Only need to be run for new teams)

2. ``get_season_results.py`` : Takes the file from the previous step ``teams_and_seasons.csv`` and returns a file with the number of points they reach in each season ``teams_and_seasons.csv``

3. ``get_stats.py`` : This file takes the ``teams_and_seasons.csv`` file and get stats from wyscout for a specific season of each team in the file ``processed_stats_{season_column}.csv``

4. ``transfermarkt.py``: Takes the ``league_id2.csv`` file and creates a file with the transfermarkt value of teams and league for each team in the file ``team_market_value.csv``


## Web scrapping stats

1. ``process_octoparse.py``: Takes the file from octoparse that needs manual download and create ratings adding some randomness to the stats.

### Automate process

Start by downloading selenium on your terminal or from the requirements

The way this works is by using selenium to use a browser, in this case chrome. We need the driver for chrome, you can download it from here:

https://sites.google.com/chromium.org/driver/


# Friends Of Tracking

This is a project for football analytics the main goal is to have an evaluation of players across a season using the VAEP metrics, which provides a better understanding of the value of a player in a team and their implication in the teams performance. 

The main idea is to have a pipeline that takes the data from the Wyscout API and process it to obtain the VAEP metrics for each player in a team.

Pipeline:

1. ``process_data.py``: Takes the data from the Wyscout API and store in h5 files as: teams.h5, palyers.h5, actions.h5, features.h5, labels.h5