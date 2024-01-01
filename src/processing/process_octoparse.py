import json
import numpy as np
import pandas as pd

# Load the JSON file
with open('data/raw/Analyst Power Rankings Interactive.json') as f:
    data = json.load(f)
df_oct = pd.DataFrame(data)

# use seed
np.random.seed(42)
df_oct['random_number'] = np.random.randint(0, 100, df_oct.shape[0])
df_oct['RATING'] = df_oct['RATING'].astype(float)

# Calculate the weight of each column
rating_weight = 0.99
random_weight = 1 - rating_weight

# Calculate the mean
df_oct['mean'] = np.average(df_oct[['RATING', 'random_number']],
                             weights=[rating_weight, random_weight], axis=1).round(4)

df_oct['rank'] = df_oct['mean'].rank(ascending=False).astype(int)

df_oct['RATING_AGG'] = (df_oct['mean'].rank(pct=True) * 100).round(2)

df_oct = df_oct[['TEAM', 'RATING', 'mean', 'rank', 'RATING_AGG']].sort_values(by='mean', ascending=False).reset_index(drop=True)

df_oct.to_csv('data/processed/processed_power_rankings.csv', index=False)

# export as json
df_oct.to_json('data/processed/processed_power_rankings.json', orient='records')