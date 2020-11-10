package src.main.java.com.jinghang.spark24.day13

/**
 * create by young
 * date:20/10/30
 * desc:
 */
object Demo02BinarySearch {

  def search(arr: Array[Int], num: Int): Int = {

    var start = 0
    var end = arr.length-1
    var mid = 0

    while (start <= end){
      mid = (start+end)/2
      if (arr(mid) == num){
        return mid
      }else if(arr(mid) > num){
        end = mid -1
      }else if(arr(mid) < num){
        start = mid+1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {

    //前提数据有序
    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 55, 88)
    val num = 111

    println(search(arr,num))

  }
}
