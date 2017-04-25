val source = sc.wholeTextFiles("books");
val totalCount = source.flatMap(text => text._2.toLowerCase().split("\\s+")).map(word=> (word, 1)).reduceByKey(_+_)
totalCount.collect().foreach(println)
val fileCount = source.flatMap(text => text._1+","+text._2.toLowerCase().split("\\s+")).map(word=> (word, 1)).reduceByKey(_+_)