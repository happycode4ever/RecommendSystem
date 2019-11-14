import scala.util.control.Breaks.breakable
import scala.util.control.Breaks.break

object ContinueTest {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 5) {
      breakable {//breakable在循环内部就是continue，在外部就是break
//        if (i % 2 == 0){}
        for (j <- 20 to 23) {
          breakable{
            if(j%2==0) break()
            println(s"i=$i j=$j")
          }
        }
      }
    }
//    println(Option(null).isEmpty)
  }
}
