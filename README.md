# Mimir-Caveats

An implementation of [Caveats](http://cidrdb.org/cidr2020/papers/p13-brachmann-cidr20.pdf) 
(aka [Uncertainty Annotations](https://odin.cse.buffalo.edu/papers/2019/SIGMOD-UADBs.pdf)) for
Spark/SparkSQL.

## Caveats 
Outliers, corner cases, missing data, or other suspicious records are subject to interpretation.  
When preparing a dataset, you may not always know what that interpretation should be.  Perhaps 
you're [missing an entire day of data due to a hurricane](https://vgc.poly.edu/~juliana/publications.html).
Do you impute the data?  Do you leave the data as is?  It all depends on how you're going to use
the data.  

Caveats solve this problem by letting you annotate *potentially* invalid data with the assumptions 
you're making.  Caveats are propagated through datasets: Data values resulting from a filter or 
map (or a `SELECT` query) are going to be annotated with any caveats on the values used to generate 
them.  At any point, you can query the caveats on your dataset.

