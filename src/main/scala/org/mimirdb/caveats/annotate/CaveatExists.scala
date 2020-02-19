package org.mimirdb.caveats.annotate

object CaveatExists
{
  def apply(
    pedantic: Boolean = true,
    ignoreUnsupported: Boolean = false,
    trace: Boolean = false
  ) = new CaveatExistsPlan(
    pedantic = pedantic,
    ignoreUnsupported = ignoreUnsupported,
    trace = trace
  )
}