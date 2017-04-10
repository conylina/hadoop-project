//access scala shell
spark-shell --master local[*] 
//execute whole file
spark-shell < aggregate.scala

wordcount.scala
--------------------
// this is on local file system. by default it's hdfs
val wordsFromFile = sc.textFile("file:///mnt/home/linaxiang/Downloads/pg4300.text")  
// flatmap: flatter an array to string collection
// split each line by space.
val wordsRDD = wordsFromFile.flatMap(l=>l.split(' '))   

val mappedWordRDD = wordsRDD.map(w => (w,1))
//reduce the word by counting occurence of unique word
val reducedWordRDD = mappedWordRDD.reduceByKey((a,b) => a+b)

reducedWordRDD.toDebugString()  // show the execution steps
reducedWordRDD.first  // return the first value to know how the result looks like. 

// tab delimit the fields of the output
val outputRdd = reducedWordRDD.map(x => x._1 + "\t" + x._2)
val sortedRDD = outputRDD.sortBy(a => a)
sortedRDD.saveAsTextFile("/user/linaxiang/output/scala/pg4300")
//need to use double quotes for path. the output file path added in IBM iop /user/linaxiang/


============
//load data from HDFS and store results back to HDFS using spark
//read the /user/cloudera/rawdata/flight_dataset/carrier/carrier.csv file from hdfs
//convert all records in the file to a tab-delimited format and save back to hdfs at /user/cloudera/rawdata/flight_dataset/carrier_tabbed 
//also remove all quotes in each field as well as the head
//full path in cloud: hdfs://iop-bi-master.imdemocloud.com:8020/user/linaxiang/rawdata/training/airline_performance/carriers/carriers.csv

val contentRDD = sc.textFile("/user/linaxiang/rawdata/training/airline_performance/carriers/carriers.csv")
//remove header
val filteredRDD = contentRDD.filter (l=> !l.startsWith("Code"))
//remove quotes
val qremoveRDD = filteredRDD.map(l => l.replace("\"", ""))
//split by comma and deparate by tab
val splittedRDD = qremoveRDD.map(l=> l.split(",")).map(a=>a(0) + "\t" + a(1))
//save to file
splittedRDD.saveAsTextFile("/user/linaxiang/rawdata/training/airline_performance/carrier_tabbed")
//IBM iop environment, without /user/linaxiang as saving path, get perission denied 
// after adding it, the output goes to this folder: /user/linaxiang/user/linaxiang/rawdata/training/airline_performance/carrier_tabbed/

filter.scala
=======================================
//read the /user/cloudera/rawdata/flight_dataset/flight file from hdfs save as tab delimited text file to hdfs
//filter flights data for April, get date, flight_num, date, departure, arrival
val contentRDD = sc.textFile("/user/linaxiang/rawdata/training/airline_performance/flights/2008.csv")
//remove header
val filteredContentRDD = contentRDD.filter (l=> !l.startsWith("Year"))
//map each line and filter by the second field of each line after splitting
//val fieldsRDD = filteredContentRDD.map(l=>l.split(',')).map(fs => (fs(2),fs)) 
val fieldsRDD = filteredContentRDD.keyBy(l=>l.split(',')(1)) // keyBy get a key field with specified field, other line content is kept in return
val filterFieldsRDD = fieldsRDD.filter(t=>t._1 == "4").map(t=>t._2) //t._1 is the key, t._2 is value part with the whole line
//present output, extracted flightnum, date, orig and dest
val outRDD = filterFieldsRDD.map(l=>l.split(",")).map(a=>a(9)+"\t" + a(2) + "/" + a(1) + "/" + a(0) + "\t" + a(16) + "\t" + a(17))
//save to output
outRDD.saveAsTextFile("/user/linaxiang/rawdata/training/airline_performance/flight_april_only")
//path is correct in this output with no duplicated/user/linaxiang. 
//the output has 6 files but only part-00001 has value. others length 0


aggregate.scala
========================================
//carrier code, sum_delay, avg_departInMin for each carrier
val contentRDD = sc.textFile("/user/linaxiang/rawdata/training/airline_performance/flights/2008.csv")
//remove header
val filteredContentRDD = contentRDD.filter (l=> !l.startsWith("Year"))
//split and extract only two fields
val fieldsRDD = filteredContentRDD.map(l=>l.split(',')).map(fs => (fs(8), fs(15))) // extract two fields carrier code and DepDelay 
//reduce
val reduceSumRDD = fieldsRDD.reduceByKey((a,b) => a+b)
val outputSumRDD = reduceSumRDD.map(t=>t._1 + "\t" + t._2)
//saving the sum
outputSumRDD.saveAsTextFile("/user/linaxiang/rawdata/training/airline_performance/flight_depart_sum_delay")

//combineByKey: initializer, measurer, combiner
// v -> initial value (v,1) 1 as count; acc (v,1) => acc._1 + v get value sum, acc._2+1 count increase
// acc1, acc2: acc1._1+acc2._1, value sum, acc1._2+acc2._2 count sum
val combineRDD = fieldsRDD.combineByKey((v =>(v,1)), (acc:(Int,Int),v:Int) => acc._1+v, acc._2+1), (acc1:(Int,Int),acc2:(Int,Int)) => acc1._1+acc2._1, acc1._2+acc2._2))
//compute average
val valueRDD = combineRDD.map(a=>a._1+"\t"+(a._2._1.toFloat/a._2._2)) //calculate average
//save 
valueRDD.saveAsTextFile("/user/linaxiang/rawdata/training/airline_performance/flight_depart_avg_delay")


join.scala
==============================================
//flightnum, carrier name
val contentRDD = sc.textFile("/user/linaxiang/rawdata/training/airline_performance/flights/2008.csv")
//remove header
val filteredContentRDD = contentRDD.filter (l=> !l.startsWith("Year"))
//split and extract two fields
val fieldsRDD = filteredContentRDD.map(l=>l.split(',')).map(fs => (fs(8), fs(9))) // extract two fields carrier and flight_num

val carriercontentRDD = sc.textFile("/user/linaxiang/user/linaxiang/rawdata/training/airline_performance/carrier_tabbed")
//map each line and filter by the second field of each line after splitting
val carrierfieldsRDD = carriercontentRDD.map(l=>l.split("\t")).map(fs => (fs(0), fs(1))) // year & month

//join both fieldsRDD
val joinedRDD = fieldsRDD.join(carrierfieldsRDD) //(key, (fs(1), fs(9))) flight number and carrier
//output presentation
val outputRDD = joinedRDD.map(v=>v._2._1+ "\t" +v._2._2) //retrieve the value in the value tuple
//save to output
outputRDD.saveAsTextFile("/user/linaxiang/rawdata/training/airline_performance/flight_carrier_name")


sort.scala
================================================
//the sum delay result
val contentRDD = sc.textFile("/user/linaxiang/rawdata/training/airline_performance/flight_depart_sum_delay")
//map each line and filter by the second field of each line after splitting
val fieldsRDD = contentRDD.map(l=>l.split('\t')).map(fs => (fs(0), fs(1).toInt)) 
//sort by the second value of the typle in deccending order
val sortedRDD = fieldsRDD.sortBy(a=> -a._2)
sortedRDD.saveAsTextFile("/user/linaxiang/rawdata/training/airline_performance/flight_depart_sum_sorted")