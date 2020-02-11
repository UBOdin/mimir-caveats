## Design Questions

#### Caveat encoding

How do we represent caveats.  The easiest thing would be to append a column called "caveats" or 
optionally some user-defined column name.  However, what type should this be?  My initial gut 
thought is to use a simple bit vector, but that's not going to work with, e.g., range annotations.
A more general approach would be to have a custom data type (I guess we'd need some sort of encoder
defined for this type) that could also be a bit more expressive and provide a bit more forward
compatibility.