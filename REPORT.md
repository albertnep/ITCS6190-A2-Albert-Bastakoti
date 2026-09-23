# Assignment #2 — Report

**Name: Albert Bastakoti**
**Student ID: 801291837**
**Email: abastako@charlotte.edu**

---

## Design

I used Design A for this. 

The mapper emits each document id as the key and its unique words, cleaned and seperated by spaces as the value. this outputs each document grouped with all of the words in it appearing once.

The reducer receives one document id and its words set. reduce() will store all the documents. Every pair is computed using jaccard similarity in cleanup(). It will compute using the intersection size divided by rhe union size, and output only pairs that share words.

My Driver is different from the L4 Controller because it uses my document Mapper and Reducer and sets the job to use one reducer. I did not add a combiner because the Reducer needs to see all documents before comparing them.

---

## How I ran it


```bash
docker compose up -d

mvn clean package

docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/tmp/
docker cp shared-folder/input/data/small_dataset.txt resourcemanager:/tmp/
docker cp shared-folder/input/data/dataset.txt resourcemanager:/tmp/

docker exec -it resourcemanager bash
cd /tmp

hadoop fs -mkdir -p /input/data
hadoop fs -put ./small_dataset.txt /input/data
hadoop fs -put ./dataset.txt /input/data
hadoop fs -ls /input/data

hadoop jar /tmp/DocumentSimilarity-0.0.1-SNAPSHOT.jar \
  com.example.controller.DocumentSimilarityDriver /input/data/small_dataset.txt /output/small_dataset

hadoop fs -cat /output/small_dataset/*

hadoop jar /tmp/DocumentSimilarity-0.0.1-SNAPSHOT.jar \
  com.example.controller.DocumentSimilarityDriver /input/data/dataset.txt /output/dataset

hadoop fs -cat /output/dataset/*
hadoop fs -cat /output/dataset/* | wc -l

hdfs dfs -get /output /tmp/
exit

docker cp resourcemanager:/tmp/output/. shared-folder/output/

docker compose down
```

---

## Output

### `small_dataset.txt` (3 lines)

```
Document1, Document2 Similarity: 0.18
Document1, Document3 Similarity: 0.20
Document2, Document3 Similarity: 0.10

```

### `dataset.txt` (66 lines)

```
Doc01, Doc02 Similarity: 0.16
Doc01, Doc03 Similarity: 0.13
Doc01, Doc04 Similarity: 0.07
Doc01, Doc05 Similarity: 0.10
Doc01, Doc06 Similarity: 0.09
Doc01, Doc07 Similarity: 0.11
Doc01, Doc08 Similarity: 0.10
Doc01, Doc09 Similarity: 0.11
Doc01, Doc10 Similarity: 0.09
Doc01, Doc11 Similarity: 0.07
Doc01, Doc12 Similarity: 0.19
Doc02, Doc03 Similarity: 0.20
Doc02, Doc04 Similarity: 0.13
Doc02, Doc05 Similarity: 0.10
Doc02, Doc06 Similarity: 0.09
Doc02, Doc07 Similarity: 0.06
Doc02, Doc08 Similarity: 0.09
Doc02, Doc09 Similarity: 0.05
Doc02, Doc10 Similarity: 0.10
Doc02, Doc11 Similarity: 0.06
Doc02, Doc12 Similarity: 0.14
Doc03, Doc04 Similarity: 0.17
Doc03, Doc05 Similarity: 0.11
Doc03, Doc06 Similarity: 0.08
Doc03, Doc07 Similarity: 0.16
Doc03, Doc08 Similarity: 0.11
Doc03, Doc09 Similarity: 0.07
Doc03, Doc10 Similarity: 0.10
Doc03, Doc11 Similarity: 0.12
Doc03, Doc12 Similarity: 0.11
Doc04, Doc05 Similarity: 0.09
Doc04, Doc06 Similarity: 0.11
Doc04, Doc07 Similarity: 0.18
Doc04, Doc08 Similarity: 0.09
Doc04, Doc09 Similarity: 0.08
Doc04, Doc10 Similarity: 0.10
Doc04, Doc11 Similarity: 0.09
Doc04, Doc12 Similarity: 0.09
Doc05, Doc06 Similarity: 0.20
Doc05, Doc07 Similarity: 0.14
Doc05, Doc08 Similarity: 0.15
Doc05, Doc09 Similarity: 0.07
Doc05, Doc10 Similarity: 0.13
Doc05, Doc11 Similarity: 0.14
Doc05, Doc12 Similarity: 0.11
Doc06, Doc07 Similarity: 0.17
Doc06, Doc08 Similarity: 0.15
Doc06, Doc09 Similarity: 0.08
Doc06, Doc10 Similarity: 0.10
Doc06, Doc11 Similarity: 0.12
Doc06, Doc12 Similarity: 0.13
Doc07, Doc08 Similarity: 0.15
Doc07, Doc09 Similarity: 0.07
Doc07, Doc10 Similarity: 0.08
Doc07, Doc11 Similarity: 0.12
Doc07, Doc12 Similarity: 0.11
Doc08, Doc09 Similarity: 0.19
Doc08, Doc10 Similarity: 0.13
Doc08, Doc11 Similarity: 0.22
Doc08, Doc12 Similarity: 0.12
Doc09, Doc10 Similarity: 0.13
Doc09, Doc11 Similarity: 0.12
Doc09, Doc12 Similarity: 0.13
Doc10, Doc11 Similarity: 0.12
Doc10, Doc12 Similarity: 0.12
Doc11, Doc12 Similarity: 0.11

```

---

## Analysis

The most similar pair is Doc08 and Doc11 with a similarity of 0.22. This makes sense because Doc08 discusses Apache Spark and distributed processing while Doc11 discusses Spark MLlib and machine learning on a cluster. The least similar pair is Doc02 and Doc09 with a similarity of 0.05. This also makes sense because Doc02 focuses on virtualization while Doc09 focuses on Spark DataFrames so they have fewer subject relating words in common.

The score values are low because each document contains many unique technical words and the documents are short. A shared words is divided by the larger union of words which lowers the jaccard
value. Removing common stop words like "the" "and" "is" and "with" during the tokenization step would make the results more specific to the relevant topic relating words.


---

## Scalability

Design A does not scale well to a very large collection because one reducer stores every document and its word set in memory. if we had a million documents the reducer could run out of memory. It also compares every document pair, which would require about one trillion comparisons. All documents are sent to one reducer instead of being processed in parallel so it would also be slow.

Design B would spread the work across reducers by using each word as a key and sending the list of
documents containing that word to the reducer. The reducer could create candidate document pairs
for each shared word and another pass could combine the shared word counts with document sizes to
calculate jaccard similarity. Design B avoids storing the whole collection in one reducer.


---

## Problems and fixes

## Problems and fixes

I first received this error because the input file was not uploaded to HDFS:

`InvalidInputException: Input path does not exist: hdfs://namenode/input/data/small_dataset.txt`

I fixed it by copying the dataset into the container and uploading it to `/input/data` in HDFS.

I also received this error:

`Type mismatch in value from map: expected org.apache.hadoop.io.NullWritable, received org.apache.hadoop.io.Text`

The Mapper outputs `Text` values, but my Driver was using `NullWritable` for both map and final output values. I fixed this by setting the map output value to `Text` and keeping the final reducer output value as `NullWritable`. After rebuilding and copying the updated JAR the small dataset produced the expected output.



---

## Use of generative AI

The author acknowledges the use of ChatGPT in the preparation of this assignment. ChatGPT was used to provide guidance on string manipulation functions and on working with mappers and sets. It was also used to help guide and review my approach while working on the assignment; however, the coding and implementation were done by me.


