package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ).withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length    = 100000000
    val chars     = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig.measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig.measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    @scala.annotation.tailrec
    def helper(i: Int, open: Int): Boolean = {
      if (open < 0) false
      else if (i == chars.length) open == 0
      else {
        chars(i) match {
          case '(' ⇒ helper(i+1, open + 1)
          case ')' ⇒ helper(i+1, open - 1)
          case _   ⇒ helper(i+1, open)
        }
      }
    }
    helper(0, 0)
  }

  // )))((((  ))))((((

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @scala.annotation.tailrec
    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      if (idx == until) (arg1, arg2)
      else {
        chars(idx) match {
          case ')' ⇒
            if (arg2 > 0) traverse(idx + 1, until, arg1, arg2 - 1) else traverse(idx + 1, until, arg1 + 1, arg2)
          case '(' ⇒ traverse(idx + 1, until, arg1, arg2 + 1)
          case _   ⇒ traverse(idx + 1, until, arg1, arg2)
        }
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = (from + until) / 2
        val ((a, b), (c, d)) = parallel(reduce(from, mid), reduce(mid, until))
        if (b >= c) (a,  b - c + d)
        else (a - b + c, d)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
