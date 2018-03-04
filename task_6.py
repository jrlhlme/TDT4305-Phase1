from pyspark import SparkContext
from operator import add
import re

def main():
    sc = SparkContext("local", "test")

    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    records = file.map(lambda x: x.split("\t"))
    outputString = "word\tfrequency\n"

    # 6 10 most frequent words
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

    words = records \
        .filter(lambda x: x[2] == "US")\
        .map(lambda s: wordSequence(re.findall(r'((www|http)(\S*)|\w+)', s[10]))).flatMap(lambda x: x)\
        .map(lambda s: (s, 1)).reduceByKey(add)

    wordsRanked = sorted(words.collect(), key=lambda s: (-s[1], s[0]))
    for i in range(10):
        outputString += (str(wordsRanked[i][0]) + "\t" + str(wordsRanked[i][1]) + "\n")

    newFile = open("result_6.tsv", "w")
    newFile.write(outputString)
    newFile.close()


main()