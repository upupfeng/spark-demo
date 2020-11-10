package com.upupfeng.lib.util

import scala.util.Random

/**
  * @Date: 2019/10/11 19:51
  */
object Generator {

  val UPPER = 255
  val random = new Random()

  def rand1or2: Int = if (random.nextInt(UPPER) % 2 == 0) 2 else 1

  def ri: Int = random.nextInt(UPPER)

  def randIp: String = s"${ri}.${ri}.${ri}.${ri}"

  def main(args: Array[String]): Unit = {
    println(randIp)
  }
}
