## Design Questions

#### Caveat encoding

How do we represent caveats?  The easiest thing would be to append a column called "caveats" or
optionally some user-defined column name.  However, what type should this be?  My initial gut
thought is to use a simple bit vector, but that's not going to work with, e.g., range annotations.
A more general approach would be to have a custom data type (I guess we'd need some sort of encoder
defined for this type) that could also be a bit more expressive and provide a bit more forward
compatibility.

*Tentative Solution*: A nested struct as follows
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

#### Range Caveats

*Tentative Solution*: A nested struct as follows
```
__CAVEATS: {
  ROW: {
    LB: Int,
    BG: Int,
    UB: Int
  },
  COLUMN: {
    attr1: {
      LB: TypeOf(attr1),
      UB: TypeOf(attr1),
    }
    ...,
    attrN: {
      LB: TypeOf(attrN),
      UB: TypeOf(attrN),
    }
  }
}
```

- we probably should implement an translation that duplicates tuples based on the BG
  annotation
- currently this assumes that min (`least`) and (`max`) operations and comparisons are well-defined for the datatypes of attributes. We need a registry or `trait` to identify datatypes with a total order of their domain. For types that we do not know we should fall back to boolean annotations.
- eventually we should support multiple different encoding of annotations which means either going with Oliver's idea of a UDT that can encode annotations in multiple ways or make the rewrites have pluggable annotation handling components that abstract away the annotation handling part (e.g., the bitvector one will bit-and bitvectors when joining and things like this). The custom type will keep the rewriter simpler, but may add some overhead at runtime, but probably not enough to worry about.
