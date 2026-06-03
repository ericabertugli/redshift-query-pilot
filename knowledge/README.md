# Knowledge Base

This folder contains YAML files with table and column descriptions. All files use a single format — `sync_knowledge.py` parses every `*.yml` file in this directory and loads descriptions into `catalog.db`.

Multiple files can describe the same table or column. When they do, all descriptions are stored and presented with their source file name as attribution.

## YAML Format

```yaml
tables:
  - name: orders                    # required — table name
    database: my_glue_db            # optional — for disambiguation
    description: >                  # optional — table-level description
      Main orders table. One row per order placed by a client.
    columns:                        # optional
      - name: status                # required
        description: >              # required
          Current order status. Values: CREATED, PAID, SHIPPED, CANCELLED.
      - name: client_id
        description: >
          FK to clients table.

  - name: deliveries
    description: "Core delivery facts table"
    columns:
      - name: id
        description: "Unique delivery identifier"
```

## File Naming

- Name files after the table or domain they describe: `orders.yml`, `deliveries.yml`, `pricing.yml`
- Group related tables in a single file when they share context
- Only `*.yml` files are parsed (not subdirectories)

## Syncing

After adding or editing files, run:

```bash
uv run python sync_knowledge.py -v
```

## Populating From External Sources

You can generate YAML files in this format from any source (Avro schemas, dbt docs, data catalogs, etc.) using conversion scripts.
