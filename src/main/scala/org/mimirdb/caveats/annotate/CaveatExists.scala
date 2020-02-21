package org.mimirdb.caveats.annotate

object CaveatExists
{
  def apply(
    pedantic: Boolean = true,
    ignoreUnsupported: Boolean = false,
    trace: Boolean = false
  ) = new CaveatExistsInPlan(
    pedantic = pedantic,
    ignoreUnsupported = ignoreUnsupported,
    trace = trace
  )
}