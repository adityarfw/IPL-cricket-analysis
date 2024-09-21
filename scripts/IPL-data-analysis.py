# Databricks notebook source
from pyspark.sql import SparkSession
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, sum, avg
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DoubleType, DecimalType, MapType
spark

# COMMAND ----------


# COMMAND ----------


# create session
spark = SparkSession.builder.appName('IPL_Data_Analysis').getOrCreate()

# COMMAND ----------

all_seasons_batting_card_schema = StructType([
    StructField('season', DoubleType(), True),
    StructField('match_id', IntegerType(), True),
    StructField('match_name', StringType(), True),
    StructField('home_team', StringType(), True),
    StructField('away_team', StringType(), True),
    StructField('venue', StringType(), True),
    StructField('city', StringType(), True),
    StructField('country', StringType(), True),
    StructField('current_innings', StringType(), True),
    StructField('innings_id', DoubleType(), True),
    StructField('name', StringType(), True),
    StructField('fullName', StringType(), True),
    StructField('runs', DoubleType(), True),
    StructField('ballsFaced', DoubleType(), True),
    StructField('minutes', IntegerType(), True),
    StructField('fours', DoubleType(), True),
    StructField('sixes', DoubleType(), True),
    StructField('strikeRate', DoubleType(), True),
    StructField('captain', BooleanType(), True),
    StructField('isNotOut', BooleanType(), True),
    StructField('runningScore', StringType(), True),
    StructField('runningOver', DecimalType(10, 2), True),
    StructField('shortText', StringType(), True),
    StructField('commentary', StringType(), True),
    StructField('link', StringType(), True),
])

all_seasons_bowling_card_schema = StructType([
    StructField('season', DoubleType(), True),
    StructField('match_id', IntegerType(), True),
    StructField('match_name', StringType(), True),
    StructField('home_team', StringType(), True),
    StructField('away_team', StringType(), True),
    StructField('bowling_team', StringType(), True),
    StructField('venue', StringType(), True),
    StructField('city', StringType(), True),
    StructField('country', StringType(), True),
    StructField('innings_id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('fullName', StringType(), True),
    StructField('overs', DoubleType(), True),
    StructField('maidens', IntegerType(), True),
    StructField('conceded', IntegerType(), True),
    StructField('wickets', IntegerType(), True),
    StructField('economyRate', DoubleType(), True),
    StructField('dots', IntegerType(), True),
    StructField('foursConceded', IntegerType(), True),
    StructField('sixesConceded', IntegerType(), True),
    StructField('wides', IntegerType(), True),
    StructField('noballs', IntegerType(), True),
    StructField('captain', BooleanType(), True),
    StructField('href', StringType(), True),
])


# COMMAND ----------

# s3://ipl-all-season-data/all_season_info/all_season_batting_card.csv

s3_batting_card = 's3://ipl-all-season-data/all_season_info/all_season_batting_card.csv'
s3_bowling_card = 's3://ipl-all-season-data/all_season_info/all_season_bowling_card.csv'

all_seasons_batting_card_df = spark.read.option('header', 'true').schema(
    all_seasons_batting_card_schema).csv(s3_batting_card)

all_seasons_bowling_card_df = spark.read.option('header', 'true').schema(
    all_seasons_bowling_card_schema).csv(s3_bowling_card)

# Exclude columns from Batting Card - runningScore, country, commentary, link
bat_all_szn_df_filtered = all_seasons_batting_card_df.drop(
    'runningScore', 'country', 'commentary', 'link')

# Exclude columns from Bowling Card - href
bowl_all_szn_df_filtered = all_seasons_bowling_card_df.drop('href')

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are the totals by each team scored grouped by city. And what is the average strike rate in each of the ground
# MAGIC

# COMMAND ----------

# Casting Runs as Integer Type! Obviously!
bat_all_szn_df_casted = bat_all_szn_df_filtered.withColumn(
    'runs', bat_all_szn_df_filtered.runs.cast(IntegerType()))

# Group by city, match, and team to calculate total runs scored and average strike rate
team_match_city_stats_df = (
    bat_all_szn_df_casted
    .filter(F.col('season') == 2023)
    # Group by city, match, and team
    .groupBy('city', 'match_id', 'current_innings')
    .agg(
        # Calculate total runs scored by each team in each match
        F.sum('runs').alias('total_runs'),
        # Calculate average strike rate for each match and city
        F.avg('strikeRate').alias('avg_strike_rate')
    )

)

# Cast avg_strike_rate to DoubleType for efficient conversion
stats_by_city_df = (
    team_match_city_stats_df
    .withColumn('avg_strike_rate', F.col('avg_strike_rate').cast(DecimalType(10, 2)))
)

# Show the result
stats_by_city_df.show(150)

num_rows = stats_by_city_df.count()
print(f"Number of rows: {num_rows}")

# printing double number of rows - shows each batting team for current innings.


# COMMAND ----------


# Convert to Pandas DataFrame
stats_by_city_pd = stats_by_city_df.toPandas()

# Check for NaN values
print(stats_by_city_pd.isnull().sum())

# Drop or fill NaN values
stats_by_city_pd = stats_by_city_pd.dropna()

# Ensure correct data types
stats_by_city_pd['total_runs'] = pd.to_numeric(
    stats_by_city_pd['total_runs'], errors='coerce')
stats_by_city_pd['avg_strike_rate'] = pd.to_numeric(
    stats_by_city_pd['avg_strike_rate'], errors='coerce')

# Create the bar plots again
plt.figure(figsize=(14, 6))
sns.barplot(x='current_innings', y='total_runs', hue='city',
            data=stats_by_city_pd, palette='Set2')
plt.title('Total Runs Scored by Each Team in Each City')
plt.xlabel('Teams')
plt.ylabel('Total Runs')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(14, 6))
sns.barplot(x='current_innings', y='avg_strike_rate',
            hue='city', data=stats_by_city_pd, palette='Set1')
plt.title('Average Strike Rate by Each Team in Each City')
plt.xlabel('Teams')
plt.ylabel('Average Strike Rate')
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------

bat_all_szn_df_filtered.show(5)
bowl_all_szn_df_filtered.show(5)

# COMMAND ----------

# Get Total Number of runs scored by CSK in 2023 Season GT v CSK match

runs_scored_each_innings = bat_all_szn_df_filtered.groupBy('match_id', 'match_name', 'innings_id').agg(
    F.sum('runs').alias('innings_runs')
)


# COMMAND ----------

# Show the total runs scored for each match by both the teams
windowSpec = Window.partitionBy('match_id')

runs_scored_total_per_match = runs_scored_each_innings.withColumn(
    'total_runs_per_match',
    F.sum('innings_runs').over(windowSpec)
)

runs_scored_each_innings_desc = runs_scored_total_per_match.orderBy(
    F.col('match_id').desc())


# COMMAND ----------

runs_scored_each_innings_desc.show(50)

# COMMAND ----------

# Find the batsman with highest strike rate, and most economical bowler for each innings in each match every year.
# match_id, match_name, venue, city, innings_id, batsman_name, batting_team, batsman_strike_rate, bowlers_name, bowling_team, bowlers_economy_rate


# batting strike rate
bat_selected_cols = all_seasons_batting_card_df.select(
    'season', 'match_id', 'match_name', 'city', 'fullName', 'current_innings', 'strikeRate')
bat_selected_cols.show(10)

# bowling economy rate
bowl_selected_cols = all_seasons_bowling_card_df.select(
    'match_id', 'fullName', 'bowling_team', 'economyRate')
bowl_selected_cols.show(10)


# COMMAND ----------

bat_cleaned_df = (
    bat_selected_cols
    # Cast 'season' to integer
    .withColumn('season', F.col('season').cast('int'))
    .dropna(subset=['season'])  # Drop rows where 'season' is null
)

bat_ordered_df = (
    bat_cleaned_df
    .orderBy(
        F.col('match_id'),      # Order by match_id in descending order
        # Order by strikeRate in descending order
        F.col('strikeRate').desc(),
        F.col('season')
    )
    .dropDuplicates(['match_id'])      # Ensure match_id is unique
)

bat_ordered_df.createOrReplaceTempView('bat_ordered_view')

bat_ordered_df.show(100)


# COMMAND ----------

# Ordering for Bowling Card

bowl_ordered_df = (
    bowl_selected_cols.orderBy(
        F.col('match_id'),
        F.col('economyRate'),
    )
    .dropDuplicates(['match_id'])
)

bowl_ordered_df.show()

bowl_ordered_df.createOrReplaceTempView('bowl_ordered_view')


# COMMAND ----------

# join both tables based on match_id
strikeRate_economyRate_summary = spark.sql("""
    SELECT * 
    FROM bat_ordered_view batCard
    JOIN bowl_ordered_view bowlCard ON bowlCard.match_id = batCard.match_id
""")

# rename columns using Method chaining
strikeRate_economyRate_summary_df = (
    strikeRate_economyRate_summary
    .withColumnRenamed('batCard.season', 'year')
    .withColumnRenamed('batCard.match_name', 'match')
    .withColumnRenamed('batCard.fullName', 'batter')
    .withColumnRenamed('batCard.current_innings', 'batting_team')
    .withColumnRenamed('batCard.strikeRate', 'strike_rate')
    .withColumnRenamed('bowlCard.fullName', 'bowler')
    .withColumnRenamed('bowlCard.bowling_team', 'bowling team')
    .withColumnRenamed('bowlCard.economyRate', 'economy rate')
    .drop('match_id')
)


strikeRate_economyRate_summary_df.show()


# Find the batsman with highest strike rate, and most economical bowler for each innings in each match in every year.
# match_id, match_name, venue, city, innings_id, batsman_name, batting_team, batsman_strike_rate, bowlers_name, bowling_team, bowlers_economy_rate

# View result below

# COMMAND ----------


# Group by city and find the maximum strike rate for each city
highest_strike_rate_city_df = (
    strikeRate_economyRate_summary_df
    .groupBy('city')
    # Get maximum strike rate per city
    .agg(F.max('strikeRate').alias('max_strike_rate'))
)

# Convert the result to a Pandas DataFrame for plotting
highest_strike_rate_city_pd = highest_strike_rate_city_df.toPandas()


# Plotting the highest strike rate per city
plt.figure(figsize=(10, 6))
plt.bar(highest_strike_rate_city_pd['city'],
        highest_strike_rate_city_pd['max_strike_rate'], color='orange')

# Add labels and title
plt.xlabel('City')
plt.ylabel('Highest Strike Rate')
plt.title('Highest Strike Rate in Each City')
# Rotate the city names for better readability
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y')

# Display the plot
plt.tight_layout()
plt.show()


# Group by city to calculate the number of unique players and the average strike rate
city_player_stats_df = (
    bat_ordered_df
    .groupBy('city')
    .agg(
        # Count of distinct batters per city
        F.countDistinct('fullName').alias('num_batters'),
        # Average strike rate per city
        F.avg('strikeRate').alias('avg_strike_rate')
    )
)

# Cast avg_strike_rate to DoubleType to avoid the DecimalType conversion warning
city_player_stats_df = city_player_stats_df.withColumn(
    'avg_strike_rate', F.col('avg_strike_rate').cast(DoubleType()))

# Convert the result to a Pandas DataFrame for plotting
city_player_stats_pd = city_player_stats_df.toPandas()

# Plotting the number of players and their average strike rate for each city
fig, ax1 = plt.subplots(figsize=(12, 6))

# Plot number of players
ax1.bar(city_player_stats_pd['city'], city_player_stats_pd['num_batters'],
        color='skyblue', label='Number of Batters')
ax1.set_xlabel('City')
ax1.set_ylabel('Number of Batters', color='skyblue')
ax1.tick_params(axis='y', labelcolor='skyblue')
ax1.set_xticklabels(city_player_stats_pd['city'], rotation=45, ha='right')

# Create a secondary axis for average strike rate
ax2 = ax1.twinx()
ax2.plot(city_player_stats_pd['city'], city_player_stats_pd['avg_strike_rate'],
         color='orange', marker='o', label='Average Strike Rate')
ax2.set_ylabel('Average Strike Rate', color='orange')
ax2.tick_params(axis='y', labelcolor='orange')

# Add a title and grid
plt.title('Number of Players and Average Strike Rate by City')
ax1.grid(axis='y')

# Display the plot
plt.tight_layout()
plt.show()
