from pyspark import SparkContext
from operator import add
import re

def main():
    sc = SparkContext("local", "test")

    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    records = file.map(lambda x: x.split("\t"))
    outputString = "place_name\tword1\tword2\tword3\tword4\tword5\tword6\tword7\tword8\tword9\tword10\n"

    stopwords = []
    with open("/home/jarlholme/PycharmProjects/untitled2/task_files/stop_words.txt") as inputfile:
        for line in inputfile:
            stopwords.append(line.strip().split('\n')[0])

    def wordSequence(strList):
        retList = []
        for e in strList:
            e = e[0].lower()
            if len(e) > 2 and e not in stopwords:
                retList.append(e)
        return retList

    usCities = records.filter(lambda x: x[2] == "US" and x[3] == "city").map(lambda s: (s[4], 1)).reduceByKey(add)
    toptenuscities = [sorted(usCities.collect(), key=lambda s: (-s[1], s[0]), reverse=False)[i] for i in range(10)]

    for city in toptenuscities:
        topwordsforcity = records.filter(lambda s: s[4] == city[0]) \
            .map(lambda s: wordSequence(re.findall(r'((www|http)(\S*)|\w+)', s[10]))).flatMap(lambda x: x) \
            .map(lambda s: (s, 1)).reduceByKey(add)

        outputString += (str(city[0]))
        words = sorted(topwordsforcity.collect(), key=lambda s: (-s[1], s[0]), reverse=False)
        for i in range(10):
            outputString += ("\t" + words[i][0])
        outputString += "\n"

    newFile = open("result_7.tsv", "w")
    newFile.write(outputString)
    newFile.close()

main()