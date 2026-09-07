# CDC JSON Format Compatibility

DTS Unified Engine has its own CDCEvent model and native log readers. For integration/testing/bridging scenarios it also accepts two legacy JSON envelope formats:

```text
POST /api/v1/migrations/{task_id}/cdc/debezium
POST /api/v1/migrations/{task_id}/cdc/canal
```

These endpoints **do not mean DTS runs Debezium or Canal**. They are parsers only:

```text
compatible JSON envelope
        ↓
DTS normalizer
        ↓
DTS CDCEvent
        ↓
transactional apply
        ↓
DTS durable checkpoint
```

Native production paths are DTS MySQL Binlog and PostgreSQL pgoutput readers. New database log readers should implement the DTS CDC SPI instead of introducing another selectable engine.
