# Optional Python Preflight

The following is an optional read-only Databricks notebook check before building. It checks two join-critical keys without collecting the source tables into local memory. It has not been run against the user's database. It is separate from the builder prompt above and is not required inside Sigma.

```python
checks = {
    "assets": ["asset_id"],
    "failure_details": ["machine_id", "failure_time"],
}

for table, keys in checks.items():
    key_sql = ", ".join(keys)
    null_sql = " OR ".join(f"{key} IS NULL" for key in keys)
    source = f"exalabs.iot.{table}"
    assert not spark.sql(
        f"SELECT 1 FROM {source} WHERE {null_sql} LIMIT 1"
    ).take(1), f"Null logical key in {source}"
    assert not spark.sql(f"""
        SELECT {key_sql}, COUNT(*) AS n
        FROM {source}
        GROUP BY {key_sql}
        HAVING COUNT(*) > 1
        LIMIT 1
    """).take(1), f"Duplicate logical key in {source}"

print("Asset and failure-detail logical keys passed.")
```
