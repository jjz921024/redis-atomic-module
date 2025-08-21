# 🚀 Redis Atomic Module

The Redis Atomic Module provides a set of collection and list commands with enhanced atomicity and richer semantics, suitable for distributed scenarios such as concurrency control, queue consumption, and capacity limitation.

[中文说明](./README-CN.md)

## 📦 Installation

1. Build the module and load it into Redis:
   ```bash
   redis-server --loadmodule /path/to/atomic-module.so
   ```
2. Or run in redis-cli:
   ```bash
   MODULE LOAD /path/to/atomic-module.so
   ```

---

## 📝 Command List & Description

### 1️⃣ HCAS —— Atomic Compare-And-Set for Hash Fields

- **Syntax**: `HCAS key field expected_value new_value`
- **Parameters**:
  - `key`: Hash key name
  - `field`: Field name
  - `expected_value`: Expected old value
  - `new_value`: New value to set
- **Return Value**:
  - `-1`: Field does not exist
  - `1`: Update successful (old value equals expected value)
  - `0`: Update failed (old value does not equal expected value)
- **Scenario**:
  - Distributed locks, optimistic concurrency control, atomic state transitions

---

### 2️⃣ HCAD —— Atomic Compare-And-Delete for Hash Fields

- **Syntax**: `HCAD key field expected_value`
- **Parameters**:
  - `key`: Hash key name
  - `field`: Field name
  - `expected_value`: Expected old value
- **Return Value**:
  - `-1`: Field does not exist
  - `1`: Delete successful (old value equals expected value)
  - `0`: Delete failed (old value does not equal expected value)
- **Scenario**:
  - Safe unlocking for distributed locks (prevent accidental unlock)

---

### 3️⃣ LPUSHRING / RPUSHRING —— Ring Queue (Auto Trimming)

- **Syntax**:
  - `LPUSHRING key len value`
  - `RPUSHRING key len value`
- **Parameters**:
  - `key`: List key name
  - `len`: Maximum length (positive integer)
  - `value`: Element to insert
- **Return Value**:
  - Not full: Returns the length after insertion
  - Full: Returns the list of trimmed elements
- **Scenario**:
  - Fixed-capacity queue, sliding window, LRU cache
  - Store the latest n records for a user

    <img src="./img/list_ring.png" alt="list_ring" style="zoom:67%;" />

---

### 4️⃣ LPUSHNF / RPUSHNF —— Safe Insert into Non-Full Queue

- **Syntax**:
  - `LPUSHNF key len value [value ...]`
  - `RPUSHNF key len value [value ...]`
- **Parameters**:
  - `key`: List key name
  - `len`: Maximum length (positive integer)
  - `value`: Element to insert
- **Return Value**:
  - Success: Returns the length after insertion
  - Full or insufficient capacity: Returns a negative number, absolute value indicates required capacity for this insertion
- **Scenario**:
  - Fixed-capacity queue, flow control, rate limiting
  - Semaphore

---

### 5️⃣ LPOPIF / RPOPIF —— Conditional Safe Queue Consumption

- **Syntax**:
  - `LPOPIF key [eq/ne] value`
  - `RPOPIF key [eq/ne] value`

- **Parameters**:
  - `key`: List key name
  - `eq/ne`: Comparison operator
  - `value`: Value to match at head/tail

- **Return Value**:
  - Match: Pops and returns the element
  - No match: Returns `0`
  - Empty queue: Returns `nil`

- **Scenario**:
  - Exactly-once consumption, idempotent queue

    <img src="./img/duplicate_pop.png" alt="duplicate_pop" style="zoom: 67%;" />

---

### 6️⃣ ZPOPMAXIF / ZPOPMINIF —— Safe Consumption When Score Matches Condition

- **Syntax**:
  - `ZPOPMAXIF key [gt/lt/gte/lte/eq/ne] score`
  - `ZPOPMINIF key [gt/lt/gte/lte/eq/ne] score`
- **Parameters**:
  - `key`: List key name
  - `gt/lt/gte/lte/eq/ne`: Comparison operator
  - `score`: Score to match
- **Return Value**:
  - Match: Returns an array of element and score
  - No match: Returns `0`
  - Empty queue: Returns an empty array
- **Scenario**:
  - **Fair distributed lock**
    
    As a waiting queue, instances write timestamps as scores when acquiring locks. If an instance crashes, other clients can check if the head instance has timed out and remove its element, preventing the lock from being held indefinitely.
    Without this atomic command, in concurrent scenarios, multiple clients may peek the head element, determine the condition is met, and simultaneously pop, mistakenly deleting multiple elements.
---

### 7️⃣ HSETEX / LPUSHEX / RPUSHEX —— Collection/List Operations with Expiry

- **Syntax**:
  - `HSETEX key field value [field value ...] [EX/EXAT/PX/PXAT] time`
  - `LPUSHEX key value [value ...] [EX/EXAT/PX/PXAT] time`
  - `RPUSHEX key value [value ...] [EX/EXAT/PX/PXAT] time`
  - `SADDEX key value [value ...] [EX/EXAT/PX/PXAT] time`
- **Parameters**:
  - `key`: Key name
  - `value`: Element to insert
  - `EX seconds`: Set expiry time in seconds
  - `PX milliseconds`: Set expiry time in milliseconds
  - `EXAT timestamp-seconds`: Set expiry at specified Unix time (seconds)
  - `PXAT timestamp-milliseconds`: Set expiry at specified Unix time (milliseconds)
- **Return Value**:
  - Same as the corresponding command without expiry. For example, `HSETEX` behaves like `HSET`, `LPUSHEX` like `LPUSH`
- **Scenario**:
  - Collection/queue writes with expiry, avoiding missing expiry in exceptional cases

---

## 📒 Scenario Types

| Type                   | Related Commands                                         | Description                                                                                                   | Scenario                |
| ---------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------- |
| Compare then operate   | HCAS, HCAD<br />LPOPIF, RPOPIF<br />ZPOPMAXIF, ZPOPMINIF | 1. Introduce CAS and CAD for hash type<br />2. Provide exactly-once consumption semantics, prevent duplicate POP | Idempotent consumption, fair distributed lock |
| List capacity control  | LPUSHRING, RPUSHRING<br />LPUSHNF, RPUSHNF               | Native Redis list operations lack capacity info; lists are unlimited, easily forming large values             | Sliding window, rate limiting, semaphore |
| Collection + expiry    | HSETEX, LPUSHEX, RPUSHEX, SADDEX                         | Equivalent to string-type SETEX command                                                                       |                         |


----

## ⚠️ Error Messages

- Expiry time must be a positive integer: Returns `ERR invalid expire time, must be a positive integer`
- Invalid comparison operator (POPIF): Returns `ERR invalid comparison flag`

---

## 🧪 Unit Tests

See `tests/atomic.tcl` for coverage of all commands, basic functionality, edge cases, and error handling.

---

## 🛠️ Contributing & Feedback

Feel free to submit Issues or PRs to improve more atomic operations and expiry semantics!