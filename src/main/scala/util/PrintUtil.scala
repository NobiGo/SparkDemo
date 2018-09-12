package util

/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 11:55
  */
object PrintUtil {
  def print(flag: String): Unit = {
    var stringBuffer: String = new String
    for (i <- 0 to 62) {
      stringBuffer += flag
    }
    println(stringBuffer.toString)
  }
}
