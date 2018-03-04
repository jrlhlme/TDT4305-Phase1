from pyspark import SparkContext
from operator import add

def main():
    sc = SparkContext("local", "test")

    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    records = file.map(lambda x: x.split("\t"))
    outputString = "place_name\ttweet_count\n"

    # 5 Tweets/city in US
    usCities = records.filter(lambda x: x[2] == "US" and x[3] == "city").map(lambda s: (s[4], 1)).reduceByKey(add)
    for city in sorted(usCities.collect(), key=lambda s: (-s[1], s[0])):
        outputString += (str(city[0]) + "\t" + str(city[1]) + "\n")

    newFile = open("result_5.tsv", "w")
    newFile.write(outputString)
    newFile.close()


main()