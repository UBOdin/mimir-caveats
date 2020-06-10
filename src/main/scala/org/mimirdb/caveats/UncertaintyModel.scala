package org.mimirdb.caveats

import org.apache.spark.sql.types.StructType

/**
  *  A non vizier model for representing uncertainty.
  */
trait UncertaintyModel {
  /**
    * Given an input schema return an adapted schema that removes any attributes that store uncertainty metadata, e.g., remove the probability attribute for TIPs.
    */
  def adaptedSchema(schema: StructType): StructType
}

/**
  * A tuple independent probabilistic database. Each tuple is associated with a probability. The possible worlds of this probablistic database are all subsets of the input tuples. Each possible world's probability is computed by multiplying the probabilities of all tuples that belong to this world and one minus the probability for all tuples that do not belong to the world.
  */
case class TupleIndependentProbabilisticDatabase(probabilityAttr: String)
extends UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType = {
    StructType(schema.fields.filterNot(_.name == probabilityAttr))
  }
}

/**
  * A block independent probablistic database which consists of a set of x-tuples. Each x-tuple is a probability distribution over a set of deterministic tuples (called alternatives) such that the sum of probabilities over the alternatives of an x-tuple is less than or equal to 1. Possible worlds are created by picking at most one alterative from every x-tuple. The probability of a possible world is computed by multiplying the probabilities of the alternatives included in the world (or one minus sum of the probabilities of all alternatives if no alterantives of an x-tuple is included).
  */
case class XDB(xtupIdAttr: String, probabilityAttr: String)
extends UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType = {
    StructType(schema.fields.filterNot(x => (x.name == xtupIdAttr) || (x.name == probabilityAttr)))
  }
}

/**
  * A VTable is a regular table where each attribute value is either a constant or a lablelled null (A null value with an identify).
  */
case class VTable(nullAttrs: Seq[String])
extends UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType = {
    StructType(schema.fields.filterNot( x => nullAttrs.contains(x.name)))
  }
}

/**
  *  A Codd table is what an SQL relation is.
  */
case class CoddTable()
    extends UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType = {
    schema
  }
}
