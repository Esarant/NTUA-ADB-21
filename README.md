# Repo for the "Advanced Database Systems" course @ NTUA.

## Assignment
In this project I was tasked to write complex queries using the RDD and SparkSQL PySpark interfaces on a [subset](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset) of the [Full MovieLens Dataset](https://grouplens.org/datasets/movielens/latest/).

The subset contains metadata for all 45,000 movies listed in the Full MovieLens Dataset. The dataset consists of movies released on or before July 2017. Data points include cast, crew, plot keywords, budget, revenue, posters, release dates, languages, production companies, countries, TMDB vote counts and vote averages.

The subset also includes files containing 26 million ratings from 270,000 users for all 45,000 movies. Ratings are on a scale of 1-5 and have been obtained from the official GroupLens website.

The SQL queries had to be tested on both CSV and PARQUET files and I had to compare the differences between them

### Query example

One of the queries i had to write was "Για τις ταινίες του είδους “Drama”, να βρεθεί το μέσο μήκος περίληψης ταινίας σε λέξεις ανά 5ετία από το 2000 και μετά"
- For movies belonging in the "Drama" genre, find the average resume word count for each 5-year period, starting from year 2000

The answer to this query was:

| Period           | AVG resume word count       | 
|---------------------|--------------|
| 2000-2004    | 	 58.84324834749764   |
| 2005-2009       | 	55.48967741935484 |
| 2010-2014               | 58.21326879271071     | 
| 2014-2019     | 50.295765877957656  |


