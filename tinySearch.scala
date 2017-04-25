case class Posting(docId: Int, tf: Int)

case class Result(docId: Int, doc: String, score: Double)

type Tokenizer = (String => Array[String])
type InvertedIndex = Map[String, List[Posting]]

case class SimpleTokenizer(regex: String = "[^a-z0-9äöüáéíóúãâêîôûàèìòùçñ]+") extends Tokenizer {
  def apply(s: String) = s.toLowerCase.split(regex)
}

class Index(val tokenizer: Tokenizer,
            private val invertedIndex: InvertedIndex = Map.empty,
            private val dataset: IndexedSeq[String] = Vector.empty) {
  def index(doc: String): Index = {
    val wordCounts = tokenizer(doc).groupBy(identity).mapValues(_.size)
    var newInverted = invertedIndex
    for((term, tf) <- wordCounts) {
      val newPostingList = Posting(dataset.size, tf) :: invertedIndex.getOrElse(term, Nil)
      newInverted += (term -> newPostingList)
    }
    new Index(tokenizer, newInverted, dataset :+ doc)
  }

  def size = invertedIndex.size
  def doc(id: Int) = dataset(id)
  def postings(term: String): List[Posting] =
    invertedIndex.getOrElse(term, Nil)
  def docCount(term: String) = postings(term).size
}

class Searcher(index: Index) {
  def docNorm(docId: Int) = {
    val docTerms = index.tokenizer(index.doc(docId))
    math.sqrt( docTerms.map( term => math.pow(idf(term), 2) ).sum )
  }

  def idf(term: String) =
    math.log(index.size.toDouble / index.docCount(term).toDouble)

  def searchOR(q: String, topK: Int = 10) = {
    val accums = new collection.mutable.HashMap[Int, Double].withDefaultValue(0D) //Map[docId -> Score]
    for (term <- index.tokenizer(q)) {
        for (posting <- index.postings(term)) {
            accums.put(posting.docId, accums(posting.docId) + posting.tf * math.pow(idf(term),2))
        }
    }
    accums.map(accumToResult).toSeq.sortWith(_.score > _.score).take(topK)
  }

  private def accumToResult(docIdAndScore: (Int, Double)): Result = {
    val (docId, score) = docIdAndScore
    Result(docId, index.doc(docId), score / docNorm(docId))
  }
}

object IndexAndSearch extends App {
  def indexFromFile(filePath: String): Index = {
    val emptyIndex = new Index(SimpleTokenizer())
    val source = sc.wholeTextFiles("books")
    val index = source.getLines.foldLeft(emptyIndex) { (accIndex, line) => accIndex.index(line) }
    source.close()
    index
  }
  val searcher = new Searcher(indexFromFile(args(0)))
  while(true) {
    println("Ready for searching:")
    searcher.searchOR(scala.io.StdIn.readLine()).foreach(println)
  }
}