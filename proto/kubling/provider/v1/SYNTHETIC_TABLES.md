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

## Existing directive mapping

| Existing directive | Typed field |
| --- | --- |
| `synthetic_parent` | `TableMetadata.synthetic.parent` |
| `synthetic_path` | `source_column` plus `path` |
| `synthetic_type=parent` | `ParentColumnBinding` with `IMMEDIATE` scope |
| `synthetic_type=parent_root` | `ParentColumnBinding` with `ROOT` scope |
| `synthetic_type=parent_array_key` | `ParentColumnBinding.identifies_parent_element=true` |
| `synthetic_parent_field` | `ParentColumnBinding.column` |
| `synthetic_allow_bulk_insert` | `SyntheticMutationMetadata.allow_bulk_insert` |
| Table `updatable` | `TableMetadata.updatable` plus operation-specific mutation flags |

Normal `TableMetadata.keys` identify synthetic rows. Parent bindings marked
with `identifies_parent_element` preserve the lineage required to mutate nested
arrays.
