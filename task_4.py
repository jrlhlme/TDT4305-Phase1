from pyspark import SparkContext
from operator import add
import time

def main():
    sc = SparkContext("local", "test")

    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    records = file.map(lambda x: x.split("\t"))
    outputString = "hour\tactivity\n"

    # 4 Hour-intervals w/most activity
    intervals = records.map(lambda s: (time.gmtime((float(s[0]) + float(s[8]))/1000)[3], 1)).reduceByKey(add).collect()
    for interval in sorted(intervals, key=lambda s: (s[1]), reverse=True):
        outputString += (str(interval[0]) + "\t" + str(interval[1]) + "\n")

    print(sorted(intervals, key=lambda s: (s[1]), reverse=True))

    newFile = open("result_4.tsv", "w")
    newFile.write(outputString)
    newFile.close()


main()