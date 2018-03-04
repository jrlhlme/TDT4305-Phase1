from pyspark import SparkContext
from operator import add
import re

def main():
    sc = SparkContext("local", "test")

    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    records = file.map(lambda x: x.split("\t"))

    outputString = ""

    # 1.a Number of Tweets:
    pairs = records.map(lambda s: (s[1], 1))
    outputString += (str(pairs.count()) + "\t")

    #1.b Distinct usernames
    userTuples = records.map(lambda s: (s[6], 1))
    outputString += (str(userTuples.reduceByKey(add).count()) + "\t")
    # print("Distinct usernames : " + str(userTuples.reduceByKey(add).count()))

    # 1.c Distinct countries
    countryTuples = records.map(lambda s: (s[1], 1))
    outputString += (str(countryTuples.reduceByKey(add).count()) + "\t")
    # print("Distinct Countries : " + str(countryTuples.reduceByKey(add).count()))

    # 1.d Distinct places
    plcTuples = records.map(lambda s: (s[4], 1))
    outputString += (str(plcTuples.reduceByKey(add).count()) + "\t")
    # print("Distinct Countries : " + str(plcTuples.reduceByKey(add).count()))

    # 1.e Distinct languages
    lanTuples = records.map(lambda s: (s[5], 1))
    outputString += (str(lanTuples.reduceByKey(add).count()) + "\t")
    # print("Distinct languages : " + str(lanTuples.reduceByKey(add).count()))

    # 1.f Minimum lat
    lats = records.map(lambda s: float(s[11]))
    outputString += (str(lats.min()) + "\t")
    # print("Mimimum latitude : " + str(lats.min()))

    # 1.g Minimum long
    lngs = records.map(lambda s: float(s[12]))
    outputString += (str(lngs.min()) + "\t")
    # print("Mimimum longitude : " + str(lngs.min()))

    # 1.h Maximum lat
    outputString += (str(lats.max()) + "\t")
    # print("Maximum latitude : " + str(lats.max()))

    # 1.i Maximum lng
    outputString += (str(lngs.max()) + "\t")
    # print("Maximum longitude : " + str(lngs.max()))

    # 1.j Avg tweet length ('chars')
    charlengths = records.map(lambda s: len(s[10]))
    outputString += (str(charlengths.mean()) + "\t")
    # print("Average tweet length (characters, including spaces) : " + str(charlengths.mean()))

    # 1.k Avg tweet length ('words')
    wordlengths = records.map(lambda s: len(re.findall(r'\w+', s[10])))
    outputString += (str(wordlengths.mean()))
    # print("Average tweet length (words) : " + str(wordlengths.mean()))

    newFile = open("result_1.tsv", "w")
    newFile.write(outputString)
    newFile.close()

main()
