# BACnet profiles

Profiles describe how BACnet objects and properties become relational tables.
The provider always loads [`standard.yaml`](standard.yaml), then compiles the
files listed under `profiles.files` in the provider configuration.

Each file contains one strict, versioned YAML document. Relative paths are
resolved from the provider configuration file:

```yaml
profiles:
  files:
    - profiles/acme-vav.yaml
```

An injected profile extends the standard profile and selects the devices to
which it applies:

```yaml
apiVersion: kubling.io/bacnet-profile/v1
id: acme-vav
extends:
  - bacnet-standard
selectors:
  - vendorIdentifier: 4242
    modelName: VAV-1000

objectTypes:
  - id: 128
    name: acme-vav

tables:
  - name: ANALOG_POINTS
    objectTypes:
      - acme-vav
    columns:
      - name: fan_mode
        property:
          id: 512
          name: fan-mode
        bacnetType: enumerated
        kublingType: STRING
        enum:
          0: off
          1: auto
          2: on
```

Selectors use exact values. Precedence is, from highest to lowest: device
instance; vendor, model and firmware; vendor and model; vendor; and finally the
built-in standard profile. A model requires a vendor and a firmware revision
requires both. Two profiles that could match the same device at the same
precedence are rejected during startup.

A device override must also include its logical `namespace` and `scopeId` so
the same BACnet Device Instance can safely exist in independent networks:

```yaml
selectors:
  - namespace: madrid.dc1
    scopeId: cooling
    deviceInstance: 1201
```

Tables with an existing name extend the inherited table. A conflicting column
must declare `override: true`; accidental changes are rejected. The compiler
also rejects inheritance cycles, unknown object types, incompatible BACnet and
Kubling types, and duplicate identifiers. Compilation order does not depend on
the order of files in the configuration.

`SCOPES` and `DEVICES` are provider-owned registry tables. Device Object
properties belong in the profile-driven `DEVICE_OBJECTS` table.

`array: true` exposes a BACnet array as a Kubling array whose element type is
`kublingType`. Properties are optional and nullable by default. Set
`required: true` for a property required by the selected device profile, and
use `nullable` when the logical schema needs an explicit override. Optional
standard properties that a device does not implement are represented as
`NULL`.

A column is read-only unless its profile declares exactly which BACnet object
types may be written. Commandable properties use the priority configured for
the logical scope; assigning `NULL` relinquishes that priority:

```yaml
      - name: present_value
        property:
          id: 85
          name: present-value
        bacnetType: real
        kublingType: DOUBLE
        write:
          commandable: true
          objectTypes:
            - analog-output
            - analog-value
```

Device PICS documentation remains authoritative. A vendor profile can override
an inherited column to narrow or remove its write policy. Writable columns are
limited to scalar boolean, integer, real, double, octet-string,
character-string and enumerated properties; arrays and structured BACnet
values remain read-only.

Profiles are data only: they cannot execute expressions, scripts or codecs.
At startup, the provider builds a deterministic union of every concrete
profile. A column that is absent from any profile contributing to the same
table becomes nullable in that union. Adding a profile that introduces tables
or columns changes the catalog and therefore requires the datasource schema to
be refreshed.

The provider also derives its semantic fragment from this compiled union.
Additional columns become typed semantic properties, and an additional table
becomes an entity with a stable identity and a relationship to its underlying
BACnet object. Profiles cannot currently declare arbitrary semantic
relationships or ontology terms.

Use `-check` to validate the union or `-print-metadata` to inspect it without
opening network sockets.
