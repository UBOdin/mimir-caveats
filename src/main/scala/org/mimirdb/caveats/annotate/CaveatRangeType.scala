package org.mimirdb.caveats.annotate

import org.mimirdb.caveats.annotate._

/**
  * Annotation type that records for each attribute
  * value an lower and upper bound across all possible worlds.
  * Furthermore, for each row we record and upper / lower bound on its
  * annotation (multiplicity) across all worlds.
  *
  * Attribute-level annotations requires attribute domains to be totally ordered.
  **/
object CaveatRangeType extends AnnotationType
{
  def defaultEncoding = CaveatRangeEncoding
}
