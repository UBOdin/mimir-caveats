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
```
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

```
println(
  df.trackCaveats                    // rewrites the query to track caveats
    .filter { $"reading".hasCaveat } // filter on readings with caveats
    .groupBy(sanitized("sensor_id")) 
    .count()                         // count the number of errors
)
```

## Usage

Import the implicit conversions to access caveat functionality
```
import org.mimirdb.caveats.implicits._
```
This adds the following methods:

#### DataFrame Implicits

```
trackCaveats: DataFrame
```
Adds an annotation column to the dataframe that you can access with `caveats` 
below.  The column is a struct with two fields: 
* `ROW`: Is true if the presence of the row in the dataframe depends on a caveat
* `ATTRIBUTE`: Struct with one field per attribute in the dataframe.  If the
  corresponding attribute is true, the value of the attribute depends on a 
  caveat

```
caveats: Column
```
A reference to the caveat annotation created by `trackCaveats`

```
listCaveats(row:Boolean = true, attributes:set[String] = true): Seq[Caveat]
```
Return every individual `Caveat` that affects the specified dataset.  Note that
this function literally loads every `Caveat` into memory all at once and may 
trigger out of memory errors on larger datasets.  For a safer approach, use 
`listCaveatSets` below.

```
listCaveatSets(row:Boolean = true, attributes:set[String] = true): Seq[CaveatSet]
```
Return `CaveatSet`s (discussed below) that can enumerate all of the caveats
on the `DataFrame`.  Unlike `listCaveats` above, this is is purely a static 
analysis of the `DataFrame`'s query plan and does not involve any query 
evaluation or memory usage scaling in the size of the data.

#### Column Implicits

```
caveat(message: (Column | String) )
```
Return a value annotated by a caveat.  If `message` is a `String`, it will be 
treated as a string literal.  If it is a `Column`, it will be cast to a string
and interpreted as normal.

```
hasCaveat
```
Must be used on a dataframe with `trackCaveats` called.  If this column is 
true, the corresponding value could be annotated with a caveat.

#### CaveatSet

A logical grouping of Caveats corresponding to one call to `Column.caveat`.  
Get the actual caveats by calling `take(n)` or `all` (note that `all` may
result in an out-of-memory error).  Count the number of caveats with `size`, 
or test for the presence of a caveat in the set with `isEmpty`.


## Operator Support

|                Operator | `trackCaveats` | `rangeCaveats` | `enumerate` |
|------------------------:|:--------------:|:--------------:|:-----------:|
|            ReturnAnswer |       ✓        |                |      ✓      |
|                Subquery |       ✓        |                |      ✓      |
|                 Project |       ✓        |                |      ✓      |
|                Generate |       ✓        |                |      ✓      |
|                  Filter |       ✓        |                |      ✓      |
|               Intersect |                |                |             |
|                  Except |                |                |             |
|                   Union |       ✓        |                |      ✓      |
|                    Join |       ✓        |                |      ✓      |
|           InsertIntoDir |       ✓        |                |      ✓      |
|                    View |       ✓        |                |      ✓      |
|                    With |                |                |             |
|    WithWindowDefinition |                |                |             |
|                    Sort |       ✓        |                |      ✓      |
|                   Range |       ✓        |                |             |
|               Aggregate |       ✓        |                |      ✓      |
|                  Window |                |                |             |
|                  Expand |                |                |             |
|            GroupingSets |                |                |             |
|                   Pivot |                |                |             |
|             GlobalLimit |       ✓        |                |      ✓      |
|              LocalLimit |       ✓        |                |      ✓      |
|           SubqueryAlias |       ✓        |                |      ✓      |
|                  Sample |                |                |             |
|                Distinct |       ✓        |                |      ✓      |
|             Repartition |                |                |             |
| RepartitionByExpression |                |                |             |
|          OneRowRelation |       ✓        |                |      ✓      |
|             Deduplicate |       ✓        |                |             |

