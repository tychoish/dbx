# dbx -- opinionated and lightweight composable golang database toolkit

This started as a very rough fork from [go-simpler.org/queries](https://pkg.go.dev/go-simpler.org/queries) (`gsq` henecforth), not because `gsq` isn't good--it's amazing--but because I wanted something slightly different.

The changes are:

- removed the interceptor implementation: database driver middleware is a cool idea, but it feels orthogonal right now.

- I wanted to use tools from my own [fun] package, for testing and for the string builder, some internals, and its iterator library (`irt`).

- I needed to be able to configure the struct tag used for interoperability, in the row iterator.

## High Level Features:

These are generally subset as `gsq`, presented with (slightly) different interfaces (in some cases.)

- a query builder to handle and produce well formed SQL statements and arguments.

- wrappers/tools for processing `database/sql` queries as generic (standard) iterators.

[fun]:https://github.com/tychoish/fun
