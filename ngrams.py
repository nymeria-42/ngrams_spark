import time
import re
import findspark
import os
import sys
from pyspark.sql import SparkSession

findspark.init('spark-3.4.0-bin-hadoop3')

def generate_ngrams(words, ngram):
    ngrams = []
    if len(words) >= ngram:
        for i in range(len(words) - (ngram - 1)):
            _ngram = [words[i + j].strip() for j in range(ngram) if len(words[i + j].strip())]
            if len(_ngram) >= ngram:
                    ngrams.append(' '.join(_ngram).strip())
    return ngrams

def count_ngrams(rdd, n, min_count):
    result = rdd.flatMap(lambda x: [(f.split("/")[-1], re.sub("[^A-Za-zÀ-ú\\s\n]", "", x[1]).lower().strip().split("\n")) for f in x[0].split(",")]) \
    .flatMap(lambda x: [(ng, x[0]) for line in x[1] for ng in generate_ngrams(line.split(" "), n)]) \
    .groupByKey() \
    .mapValues(lambda x: (len(x), list(set(x)))) \
    .filter(lambda x: x[1][0] >= min_count) \
    .sortBy(lambda x: x[0], ascending=True) \

    return result

def main():
    argv = sys.argv[1:]

    ngrams = int(argv[0])
    min_count = int(argv[1])
    input_dir = argv[2]
    output_dir = "file://" + argv[3]
    input_dir = "hdfs://localhost:9000" + input_dir

    spark = SparkSession.builder.appName('NgramCount').config('spark.ui.port', '4050').getOrCreate()

    rdd_files = spark.sparkContext.wholeTextFiles(input_dir)
    result = count_ngrams(rdd_files, ngrams, min_count)
    result.map(lambda x: x[0] + " " + str(x[1][0]) + " " + ' '.join(x[1][1])).saveAsTextFile(output_dir)
if __name__=="__main__":
    main()
