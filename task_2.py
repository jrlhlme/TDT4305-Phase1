from pyspark import SparkContext
from operator import add

def main():
    sc = SparkContext("local", "test")

    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    records = file.map(lambda x: x.split("\t"))

    outputString = ""

    # # 2 Total num of tweets/country (N/A included as '')
    countries = records.map(lambda s: (s[1], 1)).reduceByKey(add).collect()
    for ctry in sorted(countries, key=lambda country: (-country[1], country[0])):
        outputString += (str(ctry[0]) + "\t" + str(ctry[1]) + "\n")

    newFile = open("result_2.tsv", "w")
    newFile.write(outputString)
    newFile.close()

main()