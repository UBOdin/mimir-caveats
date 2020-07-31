# Mimir-Caveats

Caveats are a simple taint tracking system for Spark to help you keep track of
values that might need to be changed in the future.  For example:
* **Placeholder values**: Use caveats to keep track of placeholders you're using
  when preparing a workflow --- make sure that all your placeholders are gone
  before deploying the workflow.
* **"Good Enough for Now"**: Use caveats to keep track of approximations like
  imputed values that may need to be refined or reviewed before you make final
  decisions based on a dataset.
* **Outliers**: Use caveats to keep track of outliers that may need to be
  handled differently depending on what questions you're asking.

## Caveats
Use the `caveat` `Column` implicit to mark a value.  In the following example,
we load some sensor readings.  Erroneous readings get replaced by the previous
reading, and we mark the replaced value with a caveat.
```scala
import org.mimirdb.caveats.implicits._

val df =
  spark.read
       .format("csv")
       .option("header", "true")
       .load('sensor_readings.csv')
       .sort($"timestamp")
       .select(         // The sensor occasionally emits -999° when it errors.
         $"timestamp",
         when($"reading" < 100,
           lag($"reading", 1, lit(null))  // use the previous value
             .caveat("Sensor Error")      // and mark it with a caveat
         ).otherwise( $"reading" )
       )
```

Caveats annotate *potentially* invalid data with a string documenting the
potential error.  Caveats get propagated through dataframes as you transform
them.  For example, we could count up the number of errors:

```scala
println(
  df.trackCaveats                    // rewrites the query to track caveats
    .filter { $"reading".hasCaveat } // filter on readings with caveats
    .groupBy( df("sensor_id") )
    .count()                         // count the number of errors
)
```

## Usage

Import the implicit conversions to access caveat functionality
```scala
import org.mimirdb.caveats.implicits._
```
This adds the following methods:

#### DataFrame Implicits

```scala
trackCaveats: DataFrame
```
Adds an annotation column to the dataframe that you can access with `caveats`
below.  The column is a struct with two fields:
* `ROW`: Is true if the presence of the row in the dataframe depends on a caveat
* `ATTRIBUTE`: Struct with one field per attribute in the dataframe.  If the
  corresponding attribute is true, the value of the attribute depends on a
  caveat

```scala
caveats: Column
```
A reference to the caveat annotation created by `trackCaveats`

```scala
listCaveats(row:Boolean = true, attributes:set[String] = true): Seq[Caveat]
```
Return every individual `Caveat` that affects the specified dataset.  Note that
this function literally loads every `Caveat` into memory all at once and may
trigger out of memory errors on larger datasets.  For a safer approach, use
`listCaveatSets` below.

```scala
listCaveatSets(row:Boolean = true, attributes:set[String] = true): Seq[CaveatSet]
```
Return `CaveatSet`s (discussed below) that can enumerate all of the caveats
on the `DataFrame`.  Unlike `listCaveats` above, this is is purely a static
analysis of the `DataFrame`'s query plan and does not involve any query
evaluation or memory usage scaling in the size of the data.

#### Column Implicits

```scala
caveat(message: (Column | String) )
```
Return a value annotated by a caveat.  If `message` is a `String`, it will be
treated as a string literal.  If it is a `Column`, it will be cast to a string
and interpreted as normal.

```scala
hasCaveat
```
Must be used on a dataframe with `trackCaveats` called.  If this column is
true, the corresponding value could be annotated with a caveat.

#### CaveatSet

A logical grouping of Caveats corresponding to one call to `Column.caveat`.
Get the actual caveats by calling `take(n)` or `all` (note that `all` may
result in an out-of-memory error).  Count the number of caveats with `size`,
or test for the presence of a caveat in the set with `isEmpty`.

## Range Caveats

In addition to caveats that act as simple markers, we support a more expressive
mechanism called range-caveats where the user can express bounds on the possible
values of a caveated value. For instance, say you have a table
`Person(Name,Age)` and you know age values may differ by up to `+-5` years form
the report value, then this knowledge can be encoded for each age values using
range caveats.

```scala
import org.mimirdb.caveats.implicits._

val persons =
  spark.read
       .format("csv")
       .option("header", "true")
       .load("persons.csv")
       .select(
         $"name",
         $"age".rangeCaveat("Age imprecise",
                            $"age" - 5,
                            $"age" + 5)  // encode that age values could be between (age-5) and (age+5)
         )
       )
```

The signature of `rangeCaveat` is:

```scala
rangeCaveat(message: (Column | String), lb: Column, ub: Column)
```

Mimir takes the bounds encoded by range caveats into account when evaluating
queries to generate results that have bounds too. Continuing with the previous
example let's say the person table stored in `persons.csv` is as shown below:

| name  | age |
|-------|-----|
| Peter | 30  |
| Bob   | 35  |
| Alice | 25  |

We can compute the average age of persons over the caveated dataframe `persons` like this

```scala
val aage = persons.agg(avg($"age").as("avg_age"))
```

To tell Mimir to track range caveats, you call:

```scala
val ranged_aage = aage.trackRangeCaveats()
```

This returns a new dataframe generated by Mimir that records in additional
attributes the lower and upper bounds on each attribute value and whether rows
are certain (guaranteed to exists no matter what values within the ranges of all
caveated values is the correct one), exist in the selected guess (the values
that were caveated), and are possible (may exists for some choice of values that
fall within the ranges of range-caveated values). For example, for `range_aage`,
we would return the following table:

| avg_age | LB_avg_age | UB_avg_age | ROW_LB | ROW_BG | ROW_UB |
|---------|------------|------------|--------|--------|--------|
| 30      | 25         | 35         | 1      | 1      | 1       |

That is, based on the imprecision encoded through range caveats, the average age
of persons could be anywhere between 25 and 35.


## Operator Support

|                Operator | `trackCaveats` | `rangeCaveats`             | `enumerate` |
|------------------------:|:--------------:|:--------------------------:|:-----------:|
|            ReturnAnswer | ✓              | ✓                          | ✓           |
|                Subquery | ✓              | ✓                          | ✓           |
|                 Project | ✓              | ✓                          | ✓           |
|                Generate | ✓              |                            | ✓           |
|                  Filter | ✓              | ✓                          | ✓           |
|               Intersect |                |                            |             |
|                  Except |                | ✓                          |             |
|                   Union | ✓              | ✓                          | ✓           |
|                    Join | ✓ (inner)      | ✓ (inner)                  | ✓           |
|           InsertIntoDir | ✓              |                            | ✓           |
|                    View | ✓              |                            | ✓           |
|                    With |                |                            |             |
|    WithWindowDefinition |                |                            |             |
|                    Sort | ✓              |                            | ✓           |
|                   Range | ✓              | ✓                          |             |
|               Aggregate | ✓              | ✓ (no avg)                 | ✓           |
|                  Window |                |                            |             |
|                  Expand |                | ✓                          |             |
|            GroupingSets |                |                            |             |
|                   Pivot |                |                            |             |
|             GlobalLimit | ✓              |                            | ✓           |
|              LocalLimit | ✓              |                            | ✓           |
|           SubqueryAlias | ✓              |                            | ✓           |
|                  Sample |                |                            |             |
|                Distinct | ✓              |                            | ✓           |
|             Repartition |                |                            |             |
| RepartitionByExpression |                |                            |             |
|          OneRowRelation | ✓              | ✓                           | ✓           |
|             Deduplicate | ✓              | ✓ (only on all attributes) |             |
