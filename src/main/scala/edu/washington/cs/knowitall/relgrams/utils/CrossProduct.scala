package edu.washington.cs.knowitall.relgrams.utils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 9/6/12
 * Time: 10:51 AM
 * To change this template use File | Settings | File Templates.
 */
// Courtesy Randall Schulz
// Code based on: http://stackoverflow.com/questions/2725682/cross-product-of-2-sets-in-scala
class Crossable[X](collA:Traversable[X]) {

  def x[Y](collB: Traversable[Y]): Traversable[(X, Y)] = {
    for (a <- collA; b <- collB) yield (a, b)
  }

  def cross[Y](collB: Traversable[Y]): Traversable[(X, Y)] = {
    for (a <- collA; b <- collB) yield (a, b)
  }
}

object Crossable
{
  implicit def trav2Crossable[E1](es1: Traversable[E1]): Crossable[E1] = new Crossable[E1](es1)
}


class Pairable[X](collA:Seq[X]) {


  def pairElements[Y](collB: Seq[Y]): Seq[(X, Y)] = {
    assert(collA.size == collB.size, "CollA not the same size as CollB: " + collA.mkString(",") + " and " + collB.mkString(","))
    for (i <- 0 until collA.size) yield (collA(i), collB(i))
  }
}

object Pairable
{
  implicit def trav2Pairable[E1](es1: Seq[E1]): Pairable[E1] = new Pairable[E1](es1)
}
