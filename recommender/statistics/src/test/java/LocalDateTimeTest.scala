import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.Date

object LocalDateTimeTest {
  def main(args: Array[String]): Unit = {
    val timestamp = 1437338391*1000L
    println(new Date(timestamp).toLocaleString)
    val ldt = Timestamp.from(Instant.ofEpochMilli(timestamp)).toLocalDateTime
    println(ldt.format(DateTimeFormatter.ofPattern("yyyyMM")).toLong)
  }

}
