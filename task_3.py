from pyspark import SparkContext

def main():
    sc = SparkContext("local", "test")

    file = sc.textFile("/home/jarlholme/PycharmProjects/untitled2/task_files/geotweets.tsv")

    records = file.map(lambda x: x.split("\t"))

    outputString = "country_name\tlat\tlon\n"

    # 3 Geo-center for countries w/10< tweets
    countries = records.map(lambda s: (s[1], [1, float(s[11]), float(s[12])]))\
        .reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1], x[2] + y[2]])\
        .filter(lambda s: s[1][0] > 10).map(lambda s: (s[0], [s[1][1]/s[1][0], s[1][2]/s[1][0]]))

    for ctry in countries.collect():
        outputString += (str(ctry[0]) + "\t" + str(ctry[1][0]) + "\t" + str(ctry[1][1]) + "\n")


    newFile = open("result_3.tsv", "w")
    newFile.write(outputString)
    newFile.close()


main()