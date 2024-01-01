import pandas as pd
from fuzzywuzzy import process

# Sample dataframes
df1 = pd.read_csv('data/raw/team_market_value.csv')  # DataFrame with name_id and league_id
df2 = pd.read_csv('data/processed/processed_stats_season_p1.csv')  # DataFrame with officialName, name, and league_id

# Function to preprocess team names
def preprocess_name(name):
    return name.lower().replace(' ', '').replace('.', '').replace('-', '')

# Preprocess names in both dataframes
df1['name_id_processed'] = df1['name_id'].apply(preprocess_name)
df2['officialName_processed'] = df2['officialName'].apply(preprocess_name)
df2['name_processed'] = df2['name'].apply(preprocess_name)

def find_closest_match(row):
    # Filter df2 to the same league
    same_league_df = df2[df2['transfermarkt_id'] == row['league_id']]
    
    # Check if there are any teams in the same league to compare
    if same_league_df.empty:
        print(f"No teams found in the same league for {row['name_id_processed']}")
        return None

    # Get the best match for both officialName and name
    best_match_official = process.extractOne(row['name_id_processed'], same_league_df['officialName_processed'], score_cutoff=50)
    best_match_name = process.extractOne(row['name_id_processed'], same_league_df['name_processed'], score_cutoff=50)

    # Determine the closest match
    if best_match_official and best_match_name:
        if best_match_official[1] > best_match_name[1]:
            return same_league_df[same_league_df['officialName_processed'] == best_match_official[0]].iloc[0]
        else:
            return same_league_df[same_league_df['name_processed'] == best_match_name[0]].iloc[0]
    elif best_match_official:
        return same_league_df[same_league_df['officialName_processed'] == best_match_official[0]].iloc[0]
    elif best_match_name:
        return same_league_df[same_league_df['name_processed'] == best_match_name[0]].iloc[0]

    # Return None if no close match is found
    print(f"No close match found for {row['name_id_processed']}")
    return None

# Apply the matching function
df1['matched_row'] = df1.apply(find_closest_match, axis=1)

# Drop rows with no matches
df1 = df1.dropna(subset=['matched_row'])

# Extract relevant columns from the matched rows and join with df1
matched_df = pd.json_normalize(df1['matched_row'])
result_df = pd.concat([df1.drop(['matched_row', 'name_id_processed'], axis=1), matched_df], axis=1)

print(result_df.head())
result_df.to_csv('data/processed/processed_team_market_value.csv', index=False)
