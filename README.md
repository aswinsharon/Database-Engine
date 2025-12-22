# MiniDB - A SQLite-like Database Engine in C++17

A educational but realistic disk-based database engine implementation focusing on clean architecture and performance.

## Architecture Overview

```
┌─────────────────┐
│   Query Layer   │  ← SQL Parser & Executor
├─────────────────┤
│   Table Layer   │  ← Schema, Row Storage
├─────────────────┤
│   Index Layer   │  ← B+ Tree Implementation
├─────────────────┤
│ Buffer Manager  │  ← Page Cache, LRU Policy
├─────────────────┤
│ Storage Layer   │  ← Disk Manager, Pages
└─────────────────┘
```

## Project Structure

```
minidb/
├── src/
│   ├── storage/          # Disk management, pages
│   ├── buffer/           # Buffer pool manager
│   ├── index/            # B+ tree implementation
│   ├── table/            # Table and row management
│   ├── query/            # Query parsing and execution
│   └── common/           # Shared utilities
├── tests/                # Unit tests
├── examples/             # Usage examples
└── CMakeLists.txt
```

## Implementation Phases

1. **Week 1**: Storage Layer (DiskManager, Page)
2. **Week 2**: Buffer Pool Manager
3. **Week 3**: B+ Tree Index
4. **Week 4**: Table & Row Storage
5. **Week 5**: Query Execution Engine
6. **Week 6**: Integration & Testing

## Build & Run

```bash
mkdir build && cd build
cmake ..
make
./minidb_example
```