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
# MAGIC SELECT track_name, streams
# MAGIC FROM users.neha_sharma.spotify_2023
# MAGIC ORDER BY streams DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT artist_name, COUNT(*) as cnt
# MAGIC FROM users.neha_sharma.spotify_2023
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY cnt DESC
# MAGIC LIMIT 10

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import matplotlib.pyplot as plt

df = spark.table("users.neha_sharma.spotify_2023")

songs_per_month = (df
                   .groupBy("released_month")
                   .agg(F.count("track_name").alias("count"))
                   .orderBy("released_month"))

songs_per_month_pd = songs_per_month.toPandas()

plt.plot(songs_per_month_pd["released_month"], songs_per_month_pd["count"], color='purple')
plt.xlabel("Month")
plt.ylabel("Number of songs released")
plt.title("Number of songs released per month")
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
