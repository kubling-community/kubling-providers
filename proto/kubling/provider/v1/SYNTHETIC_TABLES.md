# Synthetic tables

Synthetic tables are regular `TableMetadata` entries in the same
`SchemaMetadata` as their physical parent. The `synthetic` field only describes
how Kubling derives that table from a parent JSON column.

Providers neither interpret synthetic definitions nor need synthetic-specific
query or mutation implementations. They expose canonical JSON and execute the
physical parent operations requested through the existing provider API.

Kubling preserves mutable synthetic-table behavior with
`PARENT_DOCUMENT_REWRITE`: it locates the physical parent row or nested array
element, applies the insert, update or delete to the document, and sends the
resulting parent mutation to the provider.

Normal `TableMetadata.keys` identify synthetic rows. Parent bindings marked
with `identifies_parent_element` preserve the lineage required to mutate nested
arrays.

The provider contract is the source of truth for this typed metadata. Kubling
DDL directives and engine-side mutation behavior are documented in the public
[synthetic-table documentation](https://docs.kubling.com/engine/ddl#synthetic-tables).
