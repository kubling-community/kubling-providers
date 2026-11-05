# Local Oracle fixture

This fixture starts the provider with the official Oracle Database Free
container. It needs enough memory for the database and may take several minutes
to become healthy on its first start.

From `providers/oracle`, run:

```sh
cp local/.env.example local/.env
./local/run.sh run
```

The provider listens on `127.0.0.1:50051`; Oracle listens on
`127.0.0.1:1521`. The setup script creates a `KUBLING` schema in `FREEPDB1`
with quoted-identifier fixtures and a normalized sales model. The Oracle
administrative password comes from `local/.env`, which is ignored by Git.
The deterministic data set includes customers, products, orders and line items,
with enough rows to inspect engine-composed joins. `SALES_ORDER_DETAIL` provides
an equivalent read-only Oracle view for comparison with source-side join
execution. Durable XA recovery is enabled with a fixture-only resource manager
identity and journal table.

Run the automated integration check with:

```sh
./local/run.sh test
```

The check recreates the local fixture, validates schema discovery, query
pushdown, mutations and local transactions, then terminates the provider after
XA prepare. A new provider process must recover and commit that branch. The
fixture remains available afterward for inspection.

Stop and remove the fixture with:

```sh
./local/run.sh down
```

This environment is intended for local interoperability tests, not persistent
database storage.
