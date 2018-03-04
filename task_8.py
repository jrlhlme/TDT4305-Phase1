import pyspark
from pyspark.sql import Row
from pyspark.sql import SQLContext


def main():
    sc = pyspark.SparkContext("local", "test")
    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    context = SQLContext(sc)
    records = file.map(lambda x: x.split("\t"))

    rows = records.map(lambda p: Row(
        utc_time=float(p[0]),
        country_name=p[1],
        country_code=p[2],
        place_type=p[3],
        place_name=p[4],
        language_=p[5],
        username=p[6],
        user_screen_name=p[7],
        timezome_offset=float(p[8]),
        number_of_friends=int(p[9]),
        tweet_text=p[10],
        latitude=float(p[11]),
        longitude=float(p[12])
    ))

    interactabledf = context.createDataFrame(rows)
    interactabledf.registerTempTable("tweets")

    # 8.a tweet number
    tweetNumbers = context.sql("SELECT count(*) FROM tweets")
    tweetNumbers.show()

    # 8.b distinct users
    distinctUsers = context.sql("SELECT count(DISTINCT username) FROM tweets")
    distinctUsers.show()

    # 8.c distinct countries
    distinctCountries = context.sql("SELECT count(DISTINCT country_name) FROM tweets")
    distinctCountries.show()

    # 8.d distinct places
    distinctPlaces = context.sql("SELECT count(DISTINCT place_name) FROM tweets")
    distinctPlaces.show()

    # 8.e distinct languages
    distinctLanguages = context.sql("SELECT count(DISTINCT language_) FROM tweets")
    distinctLanguages.show()

    # 8.f min lat,lng
    minlatlng = context.sql("SELECT MIN(latitude), MIN(longitude) FROM tweets")
    minlatlng.show()

    # 8.g max lat,lng
    maxlatlng = context.sql("SELECT MAX(latitude), MAX(longitude) FROM tweets")
    maxlatlng.show()

main()