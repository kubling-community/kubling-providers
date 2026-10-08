# In-memory semantic fixture

`project-management.json` is an opt-in semantic fragment for the canonical
`PROJECT` and `TASK` data shipped by the in-memory provider. It is intentionally
not returned by default.

An embedded use can attach the document to that fixture:

```go
implementation := inmemory.New(inmemory.WithSemanticFragment(
    &providersdk.SemanticFragment{
        Document:  document,
        MediaType: providersdk.SemanticFragmentMediaTypeJSON,
        Version:   semanticDocumentVersion,
    },
))
```

The provider tests verify every binding against its canonical model and
retrieve the fragment through the cached gRPC server.
