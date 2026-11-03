# Local BACnet/IP fixture

This fixture runs the provider against `bacserv`, the test device from the
independent C-based [BACnet Stack](https://github.com/bacnet-stack/bacnet-stack).
It is intended for repeatable interoperability and engine integration tests;
it is not a replacement for testing physical controllers from multiple
vendors.

From `providers/bacnet`, start the device and provider with:

```sh
./local/run.sh
```

The provider listens on port `50051`. The fixture contains one static device:

- namespace: `lab`
- scope: `simulated`
- Device Instance: `1234`
- Device Object Name: `kubling-lab-device`

The upstream simulator exposes one analog input, output and value; one binary
input, output and value; one multi-state input, output and value; and several
additional BACnet objects. The provider configuration uses a static route so
the lab does not depend on Docker forwarding BACnet broadcasts.

Once the provider is registered as a Kubling data source, a basic device query
is:

```sql
SELECT namespace, scope_id, device_instance, vendor_name, model_name
FROM <bacnet_source>.DEVICES
```

`DEVICES` is read from the discovery registry, so this first query needs no
prior knowledge of the fixture keys and sends no BACnet request. Live Device
Object properties are available separately:

```sql
SELECT object_name, vendor_name, model_name, system_status
FROM <bacnet_source>.DEVICE_OBJECTS
WHERE namespace = 'lab'
  AND scope_id = 'simulated'
  AND device_instance = 1234;
```

The point projections can be checked with:

```sql
SELECT object_type, object_instance, object_name, present_value, units
FROM <bacnet_source>.ANALOG_POINTS
WHERE namespace = 'lab'
  AND scope_id = 'simulated'
  AND device_instance = 1234;
```

That query returns the three analog point families. `BINARY_POINTS` and
`MULTISTATE_POINTS` follow the same access pattern and also return three rows
each.

The fixture enables writes at BACnet priority 16. Its analog output can be
changed and read back with:

```sql
UPDATE <bacnet_source>.ANALOG_POINTS
SET present_value = 21.5
WHERE namespace = 'lab'
  AND scope_id = 'simulated'
  AND device_instance = 1234
  AND object_type = 'analog-output'
  AND object_instance = 1;

SELECT present_value
FROM <bacnet_source>.ANALOG_POINTS
WHERE namespace = 'lab'
  AND scope_id = 'simulated'
  AND device_instance = 1234
  AND object_type = 'analog-output'
  AND object_instance = 1;
```

Relinquish the same command priority by assigning `NULL` with the identical
object identity:

```sql
UPDATE <bacnet_source>.ANALOG_POINTS
SET present_value = NULL
WHERE namespace = 'lab'
  AND scope_id = 'simulated'
  AND device_instance = 1234
  AND object_type = 'analog-output'
  AND object_instance = 1;
```

The exact identity predicates are mandatory. Inputs remain read-only even
when mutations are enabled for the scope.

The registry can also drive a bounded join without hard-coding a Device
Instance:

```sql
SELECT d.device_instance, p.object_name, p.present_value, p.units
FROM <bacnet_source>.DEVICES AS d
JOIN <bacnet_source>.ANALOG_POINTS AS p
  ON p.namespace = d.namespace
 AND p.scope_id = d.scope_id
 AND p.device_instance = d.device_instance;
```

Useful lifecycle commands are:

```sh
./local/run.sh device
./local/run.sh provider
./local/run.sh status
./local/run.sh logs
./local/run.sh down
```

The simulator source revision and archive checksum are fixed in
`simulator.Dockerfile`, keeping the fixture reproducible and preventing a
mutable upstream branch from changing test behavior.
