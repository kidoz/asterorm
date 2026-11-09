# PostgreSQL interval and address values

Include `asterorm/core/types.hpp` for values and `asterorm/core/codecs.hpp` for
text conversion. Link `AsterORM::core` or the umbrella `AsterORM::asterorm` target.
Rebuild the library and consumers together: parsing now uses compiled core helpers,
including existing date, timestamp and numeric codecs. No existing value layout
changes; these are new value types and stricter malformed-input checks.

```cpp
asterorm::interval delay{.months = 1, .days = -2, .microseconds = 1500000};
auto parameter = asterorm::encode_parameter(delay);
// parameter.value is PostgreSQL interval text; parameter.error signals failure.

asterorm::network host;
auto parsed = asterorm::decode(std::optional<std::string>{"192.0.2.9/24"}, host);
if (!parsed) {
    return std::unexpected(parsed.error());
}
auto address_parameter = asterorm::encode_parameter(host);
```

`interval` retains signed 32-bit `months`, signed 32-bit `days` and signed 64-bit
`microseconds`. Its equality compares components. A month is not flattened into
30 days. Encoding uses signed units and exact microseconds. Decoding supports
PostgreSQL 16 `postgres`, `postgres_verbose`, `sql_standard` and `iso_8601` output,
including mixed signs and `ago`. No pool/session setting changes are needed.

`network` represents `inet`. `family` is `address_family::ipv4` or `ipv6`,
`address` holds 16 bytes in network order, and `prefix_length` counts network bits.
IPv4 uses the first four bytes; the other twelve must be zero. Default construction
is `0.0.0.0/32`. Numeric dotted IPv4 and full/compressed IPv6 input, including
embedded IPv4 tails, are supported. DNS names, abbreviated/leading-zero IPv4,
zone identifiers and bracketed addresses reject. Omitted prefixes mean a single
host (`/32` or `/128`); encoders always include the prefix. IPv6 output uses
compressed lowercase hex, including mapped-address tails, while preserving the
IPv6 family.

`cidr` wraps a network in `.value` and additionally rejects nonzero host bits.
It never masks a host address. `macaddr` contains six `.bytes` and uses canonical
colon-separated hex; the decoder accepts either hex letter case. Alternate native
MAC input spellings and `macaddr8` are outside this codec surface.

Mapped fields can use these types directly; `optional<T>` represents SQL NULL.
For native standalone parameters, supply SQL type context such as `SELECT
$1::interval`, `$1::inet`, `$1::cidr` or `$1::macaddr`. Repository target columns
supply that context. No vendor OIDs, hidden casts, catalog lookups or schema setup
are introduced. Codec availability does not give SQLite/MySQL native PostgreSQL
types; those backends continue to use their existing text binding rules.

Numeric values keep exact decimal/exponent text and accept the canonical special
spellings `NaN`, `Infinity`, `-Infinity`. Invalid `numeric` text fails locally;
ordinary string parameters retain server validation. Finite dates use ISO AD
calendar text with years 1 through 32767. Timestamp decoding checks calendar,
clock, complete input, representability, and numeric offsets up to `15:59:59`
(including seconds), or `Z`. Offset-less MySQL timestamps remain UTC. Fractions
accept one through six digits. Encoding truncates submicrosecond precision toward
the epoch and emits UTC with six fractional digits, including before 1970.
Non-ISO DateStyle, BC dates and standalone temporal infinities reject.

Flat arrays retain empty and nullable elements, quote scalar text when needed,
and leave output unchanged on malformed input or element failure. Nested vectors,
nested unquoted braces and all explicit dimension prefixes reject; no
multidimensional round-trip is promised. Invalid encoding through
`encode_parameter` sets `.error`, distinct from a valid optional SQL NULL.
Direct `encode` retains its existing optional-string return shape; check the
parameter error when distinguishing failure from NULL matters. Custom `codec<T>`
priority remains unchanged.

Ranges, hstore, composites and checked JSONB expressions remain planned in the
[type contract](../postgresql-type-design.md). Next is range and flat-array
composition. See [local verification](../evidence/postgres-type-foundations/README.md)
for commands and limits.
