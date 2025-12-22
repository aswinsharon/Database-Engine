# MiniDB Architecture & Design Decisions

## Core Design Principles

1. **Page-Oriented Storage**: All data stored in fixed-size pages (4KB default)
2. **Buffer Pool Caching**: Minimize disk I/O through intelligent caching
3. **B+ Tree Indexing**: Disk-aware tree structure for efficient lookups
4. **ACID Properties**: Focus on Atomicity and Durability (simplified)

## Layer-by-Layer Design

### 1. Storage Layer

**Page Structure (4KB)**
```
┌──────────────────────────────────┐
│ Page Header (24 bytes)           │
│  - page_id: 4 bytes              │
│  - page_type: 1 byte             │
│  - lsn: 8 bytes (for WAL)        │
│  - free_space_offset: 4 bytes    │
│  - tuple_count: 4 bytes          │
│  - reserved: 3 bytes             │
├──────────────────────────────────┤
│ Data Area (4072 bytes)           │
│  - Actual tuple/index data       │
└──────────────────────────────────┘
```

**DiskManager Responsibilities**
- Allocate/deallocate pages
- Read/write pages to disk
- Maintain free page list
- File I/O abstraction

**Design Decision**: Fixed-size pages simplify memory management and align with OS page sizes for efficient I/O.

### 2. Buffer Pool Manager

**Architecture**
```
┌─────────────────────────────────────┐
│  Page Table (page_id → frame_id)   │
├─────────────────────────────────────┤
│  Frame Array (in-memory pages)      │
│  [Frame 0] [Frame 1] ... [Frame N]  │
├─────────────────────────────────────┤
│  Replacer (LRU/Clock)               │
│  - Tracks page access patterns      │
│  - Evicts cold pages                │
└─────────────────────────────────────┘
```

**Key Operations**
- `FetchPage(page_id)`: Get page from buffer or disk
- `UnpinPage(page_id, is_dirty)`: Release page reference
- `FlushPage(page_id)`: Write dirty page to disk
- `NewPage()`: Allocate new page

**Design Decision**: LRU replacement policy balances simplicity and effectiveness. Pin/unpin mechanism prevents eviction of in-use pages.

### 3. B+ Tree Index

**Node Structure**
```
Internal Node:
┌────────────────────────────────────┐
│ Header: is_leaf, num_keys, parent │
├────────────────────────────────────┤
│ Keys: [k1, k2, ..., kn]           │
├────────────────────────────────────┤
│ Children: [p0, p1, ..., pn]       │
└────────────────────────────────────┘

Leaf Node:
┌────────────────────────────────────┐
│ Header: is_leaf, num_keys, next   │
├────────────────────────────────────┤
│ Keys: [k1, k2, ..., kn]           │
├────────────────────────────────────┤
│ Values: [v1, v2, ..., vn]         │
│ Next Leaf: page_id                │
└────────────────────────────────────┘
```

**Operations**
- `Insert(key, value)`: Insert with split propagation
- `Search(key)`: Binary search in nodes
- `Delete(key)`: Delete with merge/redistribute

**Design Decision**: 
- Leaf nodes linked for range scans
- Keys stored in sorted order for binary search
- Max keys per node: (PAGE_SIZE - HEADER) / (KEY_SIZE + PTR_SIZE)

### 4. Table & Row Storage

**Schema Definition**
```cpp
Column: {name, type, size, offset}
Schema: vector<Column>
```

**Row Format (Tuple)**
```
┌──────────────────────────────────┐
│ Tuple Header (8 bytes)           │
│  - size: 4 bytes                 │
│  - flags: 4 bytes (deleted, etc) │
├──────────────────────────────────┤
│ Column Data (fixed-length)       │
│  - col1, col2, ..., coln         │
└──────────────────────────────────┘
```

**Table Page Layout**
```
┌──────────────────────────────────┐
│ Page Header                      │
├──────────────────────────────────┤
│ Slot Array (grows down)          │
│  [offset_1, offset_2, ...]       │
├──────────────────────────────────┤
│         Free Space               │
├──────────────────────────────────┤
│ Tuples (grow up)                 │
│  [tuple_n, ..., tuple_2, tuple_1]│
└──────────────────────────────────┘
```

**Design Decision**: Slotted page design allows variable-length tuples and efficient space management.

### 5. Query Execution

**Execution Flow**
```
SQL String → Tokenizer → Parser → Executor → Result
```

**Supported Commands**
```sql
INSERT INTO table VALUES (v1, v2, ...);
SELECT * FROM table WHERE key = value;
DELETE FROM table WHERE key = value;
```

**Execution Plans**
- SeqScan: Full table scan
- IndexScan: B+ tree lookup
- Insert: Add tuple + update index
- Delete: Mark deleted + update index

**Design Decision**: Simple recursive descent parser for MVP. No query optimizer initially.

## Performance Considerations

1. **Buffer Pool Size**: Larger pool = fewer disk I/Os
2. **Page Size**: 4KB aligns with OS, good for most workloads
3. **B+ Tree Fanout**: Higher fanout = shorter tree = fewer I/Os
4. **Index Selectivity**: Use index when WHERE clause is selective

## Concurrency (Future Work)

- Page-level latching
- Two-phase locking for transactions
- Deadlock detection

## Crash Recovery (Future Work)

- Write-Ahead Logging (WAL)
- REDO log for durability
- Checkpointing for faster recovery