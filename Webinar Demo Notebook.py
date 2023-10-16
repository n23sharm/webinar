# Databricks notebook source
# DBTITLE 1,Initial Query
# MAGIC %sql
# MAGIC
# MAGIC select * from users.neha_sharma.spotify_2023;

# COMMAND ----------

spotify_df = _sqldf.toPandas()

display(spotify_df)

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

