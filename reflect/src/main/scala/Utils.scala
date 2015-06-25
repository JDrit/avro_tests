

object Utils {
  implicit def toBetterItrDouble(i: Iterable[Double]) = new BetterItr(i)
  implicit def toBetterItrAny(i: Iterable[Any]) = new BetterItr(i.map(_.asInstanceOf[Double]))
  implicit def toBetterItrLong(i: Iterable[Long]) = new BetterItr(i.map(_.toDouble))
  implicit def toBetterItrJavaLong(i: Iterable[java.lang.Long]) = new BetterItr(i.map(_.toDouble))
  implicit def toBetterItrJavaDouble(i: Iterable[java.lang.Double]) = new BetterItr(i.map(_.toDouble))
  def notNull[T](x: T) = x != null
}

class BetterItr[T](val i: Iterable[T]) {

  def avg()(implicit num: Numeric[T]): Double = {
    val (s, c) = i.foldLeft((num.zero, 0)) { (i, n) => (num.plus(i._1, n), i._2 + 1) }
    if (c == 0) 0
    else num.toDouble(s) / c
  }
}
