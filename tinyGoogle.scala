def f(filename: String, contents: Array[String]):List[String] =
{
var curr = List[String]()
for(i<-0 until contents.length)
{
var next = List(filename.concat(",").concat(contents(i)))
curr :::= next
}
return curr
}
def querytoken(contents: Array[String]):List[String] =
{
var curr = List[String]()
for(i<-0 until contents.length)
{
var next = List(contents(i))
curr :::= next
}
return curr
}
val source = sc.wholeTextFiles("books/")
val totalCount = source.flatMap(text => text._2.toLowerCase().split("\\s+")).map(word=> (word, 1)).reduceByKey(_+_)
//totalCount.collect().foreach(println)
//val fileCount = source.flatMap(filename => filename._1.toLowerCase().split("\\s+")).map(name=> (name, 1)).reduceByKey(_+_)
//fileCount.collect().foreach(println)
//val fileCount1 = source.reduceByKey(_.concat(",").concat(_).toLowerCase())
//fileCount1.collect()foreach(println)
//val fileCount = fileCount1.map(text => text._1.concat(",").concat(text._2))
//val arrayFile = fileCount.collect()
//var res = arrayFile.split(",",2)
//print(res(1))
//print(res(0))
//val idk = source.map(text => text._1.concat(",").concat(text._2.toLowerCase().split("\\s+"))).map(word=> (word, 1)).reduceByKey(_+_)
val fileCount = source.flatMap(text => f(text._1,text._2.toLowerCase().split("\\s+"))).map(word=> (word, 1)).reduceByKey(_+_)
//fileCount.collect()foreach(println)
//val res = fileCount1.map(fileText => fileText.split(",",2))
//res.collect()foreach(println)
//text._2.toLowerCase().split("\\s+")foreach(text._1.concat(",").concat(_))
val queryInput = sc.textFile("input.txt")
val queryTokenized = queryInput.flatMap(word => querytoken(word.toLowerCase().split("\\s+")))
val queryArray = queryTokenized.collect()
val filterFileCount = fileCount.filter(input => queryArray.toSet.contains(input._1.split(",",2)(1)))
//filterFileCount.collect()foreach(println)
val results = filterFileCount.map(strInt => (strInt._1.split(",",2)(0), strInt._2)).reduceByKey(_+_).sortBy(_._2,false)
print("\n\n\n\n\n\nResults:")
results.foreach(println)