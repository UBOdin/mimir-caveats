package org.mimirdb.caveats


import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * An encapsulation of the caveat annotations on a logical plan.
 *
 * @param  plan            An annotated logical plan that identifies rows and 
 *                         fields potentially tainted by a caveat.
 * @param  table           True if the table could be affected by a table-level
 *                         caveat.
 * @param  attributes      A set of attributes that could be affected by 
 *                         attribute-level caveats.  These are not included in 
 *                         the plan.
 * @param  order           True if it is possible for the sort order of the 
 *                         relation defined by this plan to be tainted by a 
 *                         caveat.
 * @param  safeAttributes  A set of attributes guaranteed to be free of caveats.
 * 
 * We break caveats down into four classes: 
 *  - Table:     Indicating that there are rows or attributes that could be 
 *               added to the table if assumptions change.  Table level caveats  
 *               can alsobe used to capture coarse-grained dependencies (i.e., 
 *               assumptions that do not have well-defined effects on the table)
 *  - Row:       Indicating that changing assumptions could exclude this row 
 *               from the relation.
 *  - Attribute: Indicating schema-level assumptions that could change the 
 *               structure of the attribute.
 *  - Field:     Indicating assumptions that could lead to this specific value 
 *               changing.
 *    
 * Field- and Row-level annotations are per-record, and computed by executing
 * [plan] (which also computes the result of the plan itself).  Table- and
 * Attribute-level annotations are captured by the [table] and [attributes] 
 * fields. 
 *
 * The order and safeAttributes fields are metadata used in recursive analysis 
 * of the plan.
 */

object EnumerateCaveats
{
  def apply(
    plan: LogicalPlan,
  )(
    row: Boolean = false,
    fields: Set[String] = Set(),
    order: Boolean = false,
    table: Boolean = false,
    attributes: Set[String] = Set()
  ): Seq[CaveatSet] = ???
}