package org.mimirdb.caveats.annotate

object CaveatExists
  extends AnnotationStyle
{
  def apply(
    pedantic: Boolean = true,
    ignoreUnsupported: Boolean = false,
    trace: Boolean = false,
  ) = new CaveatExistsPlan
}