## Design Questions

#### Caveat encoding

How do we represent caveats?  The easiest thing would be to append a column called "caveats" or 
optionally some user-defined column name.  However, what type should this be?  My initial gut 
thought is to use a simple bit vector, but that's not going to work with, e.g., range annotations.
A more general approach would be to have a custom data type (I guess we'd need some sort of encoder
defined for this type) that could also be a bit more expressive and provide a bit more forward
compatibility.

*Tentative Solution*: A nested struct asd follows
```
__CAVEATS: { 
  ROW: row_annotation,
  COLUMN: {
    attr1: attr1_annotation,
    ...,
    attrN: attrN_annotation
  }
}
```
This should also allow for more complex annotations down the line.

#### Caveats for complex types

Spark supports complex types.  Portions of the complex type may be caveatted
while other portions might not.  We could (potentially) get tighter bounds 
by producing a more complex bound.