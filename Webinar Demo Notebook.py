# Databricks notebook source
# DBTITLE 1,Initial Query
# MAGIC %sql
# MAGIC
# MAGIC select * from users.neha_sharma.spotify_2023;

# COMMAND ----------

spotify_df = _sqldf.toPandas()

display(spotify_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT artist_name, COUNT(*) as num_tracks
# MAGIC FROM users.neha_sharma.spotify_2023 
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY num_tracks DESC
# MAGIC LIMIT 10;

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import matplotlib.pyplot as plt

# Read data into Spark DataFrame
df = spark.table('users.neha_sharma.spotify_2023')

# Create new DataFrame with total songs by month
df_grp = df.groupBy(df['released_month'].alias('Month')).agg(F.count('track_name').alias('Total Songs')).orderBy('Month')

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_grp.toPandas()

# Plot line chart with purple color
plt.plot(df_pandas['Month'], df_pandas['Total Songs'], color='purple')
plt.title('Total Songs Released per Month')
plt.xlabel('Month')
plt.ylabel('Total Songs')
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

top10_df = spotify_df.sort_values(by="in_spotify_playlists", ascending=False).head(10)

display(top10_df[['track_name', 'artist_name', 'in_spotify_playlists']])

# COMMAND ----------

def displayTopSongsInPlaylist(platform_column) :
  top10_df = spotify_df.sort_values(by=platform_column, ascending=False).head(10)
  display(top10_df[['track_name', 'artist_name', platform_column]])

# COMMAND ----------

displayTopSongsInPlaylist("in_apple_playlists")
displayTopSongsInPlaylist("in_deezer_playlists")


# COMMAND ----------

import matplotlib.pyplot as plt


# Get the top 10 songs in each playlist by sorting based on the playlist column
spotify_top10 = spotify_df.sort_values(by='in_spotify_playlists', ascending=False).head(10)
apple_top10 = spotify_df.sort_values(by='in_apple_playlists', ascending=False).head(10)
deezer_top10 = spotify_df.sort_values(by='in_deezer_playlists', ascending=False).head(10)


# Get the average valence of the top 10 songs in each playlist
spotify_avg_valence = spotify_top10['valence_%'].mean()
apple_avg_valence = apple_top10['valence_%'].mean()
deezer_avg_valence = deezer_top10['valence_%'].mean()


# Plot the data
plt.bar(['Spotify', 'Apple Music', 'Deezer'], [spotify_avg_valence, apple_avg_valence, deezer_avg_valence], alpha=0.5)
plt.ylabel('Average Valence %')
plt.title('Average Valence of Top 10 Songs in Playlists')
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

# Get the top 10 songs in each playlist by sorting based on the playlist column
spotify_top10 = spotify_df.sort_values(by='in_spotify_playlists', ascending=False).head(10)
apple_top10 = spotify_df.sort_values(by='in_apple_playlists', ascending=False).head(10)
deezer_top10 = spotify_df.sort_values(by='in_deezer_playlists', ascending=False).head(10)

# Get the average valence and danceability of the top 10 songs in each playlist
spotify_avg_valence = spotify_top10['valence_%'].mean()
spotify_avg_danceability = spotify_top10['danceability_%'].mean()

apple_avg_valence = apple_top10['valence_%'].mean()
apple_avg_danceability = apple_top10['danceability_%'].mean()

deezer_avg_valence = deezer_top10['valence_%'].mean()
deezer_avg_danceability = deezer_top10['danceability_%'].mean()

# Plot the bar chart
fig, ax = plt.subplots(figsize=(7, 5))

labels = ['Spotify', 'Apple Music', 'Deezer']
valence_means = [spotify_avg_valence, apple_avg_valence, deezer_avg_valence]
danceability_means = [spotify_avg_danceability, apple_avg_danceability, deezer_avg_danceability]

x = np.arange(len(labels))
width = 0.35

rects1 = ax.bar(x - width/2, valence_means, width, label='Valence %')
rects2 = ax.bar(x + width/2, danceability_means, width, label='Danceability %')

ax.set_ylabel('Percentage')
ax.set_title('Average Valence and Danceability of Top 10 Songs in Playlists')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

plt.show()
