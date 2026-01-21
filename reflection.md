# Healthcare Analytics Star Schema: Analysis & Reflection
## Reality Check: When Theory Meets 500K Encounters

---

## Executive Summary

This project transformed a normalized OLTP healthcare database into a star schema for analytics. Real-world testing revealed **mixed results**: the star schema averaged 1.25x faster (not the 10-25x often claimed), with performance ranging from 2x slower to 2.4x faster depending on query pattern.

This reflection analyzes why theoretical benefits didn't fully materialize at medium scale, what worked, what didn't, and honest lessons about data warehouse design.

---

## Part 1: The Performance Reality

### Expected vs. Actual Results

| Query | Theory Claims | Actual Result | Reality |
|-------|---------------|---------------|---------|
| Q1: Monthly Encounters | 10-15x faster | **0.48x (2x slower)** | Dimension join overhead |
| Q2: Diagnosis-Procedure | 8-12x faster | **1.34x faster** | Surrogate keys help modestly |
| Q3: Readmissions | 11-15x faster | **0.98x (equal)** | Self-join dominates |
| Q4: Revenue | 20-30x faster | **2.43x faster** | Pre-aggregation works! |
| **Average** | **10-25x faster** | **1.25x faster** | Modest improvement |

### The Honest Truth

**At 500K encounters, star schema is NOT dramatically faster.** Here's why:

---

## Part 2: Why Star Schema Lost Query 1

### Query 1: Monthly Encounters (Star 2x SLOWER)

**What We Expected**:
- Pre-computed dates would eliminate DATE_FORMAT()
- Direct specialty dimension would skip provider join
- Result: 10x faster

**What Actually Happened**: 5,334ms (star) vs 2,551ms (OLTP)

**Why Star Schema Lost**:

1. **Extra Dimension Joins**
   - OLTP: 2 joins (encounters → providers → specialties)
   - Star: 3 joins (fact_encounters → dim_date, dim_specialty, dim_encounter_type)
   - **Cost**: One additional join operation

2. **Wider Fact Table**
   - OLTP encounters: ~150 bytes per row (core fields only)
   - fact_encounters: ~300 bytes per row (includes pre-aggregated metrics)
   - **Cost**: Slower table scans (less data fits in memory)

3. **Surrogate Key Overhead**
   - Must lookup date_key (20240510) from date
   - Must lookup specialty_key from specialty_id
   - **Cost**: Index lookups for every dimension

4. **OLTP Optimization**
   - MySQL optimized encounters table with proper indexes
   - DATE_FORMAT() cost is minimal with good query planner
   - Provider → specialty join uses indexed FK

**Lesson**: For simple aggregations returning small result sets, normalized schemas can outperform star schemas at medium scale.

---

## Part 3: Why Star Schema Won Query 4

### Query 4: Revenue Analysis (Star 2.4x FASTER)

**OLTP Query** (7,501ms):
```sql
-- Must join 4 tables
FROM encounters → providers → specialties + billing
-- Runtime aggregation
SUM(billing.allowed_amount)
-- Date function per row
DATE_FORMAT(encounter_date, '%Y-%m')
```

**Star Schema Query** (3,083ms):
```sql
-- Only 2 dimension joins  
FROM fact_encounters → dim_date, dim_specialty
-- Pre-aggregated revenue
SUM(f.total_allowed_amount)
-- Pre-computed month
d.month_year
```

**Why Star Schema Won**:

1. **No Billing Table Join** (biggest win)
   - OLTP must join 500K encounters to 500K billing records
   - Star has revenue pre-aggregated during ETL
   - **Benefit**: Eliminates entire table join

2. **Pre-Computed Dates**
   - OLTP runs DATE_FORMAT() on 500K rows
   - Star references pre-computed month_year
   - **Benefit**: 500K function calls eliminated

3. **Fewer Joins**
   - OLTP: 4 tables joined
   - Star: 2 tables joined
   - **Benefit**: Simpler execution plan

4. **Simple Aggregation**
   - OLTP: Runtime SUM of billing records
   - Star: SUM of pre-computed totals
   - **Benefit**: Faster aggregation

**Lesson**: Pre-aggregation delivers real benefits when queries would otherwise join and aggregate transactional tables.

---

## Part 4: The Scale Problem

### Why 500K Is The "Gray Zone"

**Too Small For Dramatic Benefits**:
- OLTP indexes still work efficiently
- B-tree lookups are fast for 500K rows
- MySQL query optimizer handles joins well

**Too Large To Ignore Patterns**:
- Revenue queries benefit from pre-aggregation
- Many-to-many queries see modest improvement
- Cartesian explosion starts becoming noticeable

### Projected Performance at 5M Encounters

| Query | 500K (actual) | 5M (projected) | Why Different? |
|-------|---------------|----------------|----------------|
| Q1 | 0.48x (slower) | 1.5-2x faster | OLTP indexes degrade; star dimensions remain constant size |
| Q2 | 1.34x faster | 3-5x faster | Cartesian explosion worsens; surrogate keys scale better |
| Q3 | 0.98x (equal) | 1.0x (still equal) | Self-join cost dominates regardless of schema |
| Q4 | 2.43x faster | 8-15x faster | Billing join becomes major bottleneck; pre-agg shines |

**Expected Average at 5M**: 3-5x faster (not 10-25x, but meaningful)

### Why Scale Matters

**OLTP Performance Degradation**:
- Index size grows (less fits in memory)
- Join selectivity decreases (more rows to filter)
- Billing table join becomes expensive
- Cache hit rates decline

**Star Schema Advantage**:
- Dimension tables stay small (constant size)
- Pre-aggregated metrics avoid transaction table joins
- Fact table partitioning becomes viable
- Column-store indexes become effective

---

## Part 5: What We Got Right

### 1. Batch Processing ETL

**Implementation**:
```python
# Load in 100K batches with commits
for batch in [1-100K, 100K-200K, 200K-300K, 300K-400K, 400K-500K]:
    INSERT INTO fact_encounters (batch)
    COMMIT
```

**Value**: ETL completed in 45-60 minutes without timeouts

**Lesson**: Never load 500K+ rows in single transaction

### 2. Temporary Lookup Tables

**Anti-Pattern** (would take hours):
```sql
INSERT INTO fact_encounters
SELECT (SELECT patient_key FROM dim_patient WHERE ...) -- 500K subqueries!
```

**Solution**:
```sql
CREATE TEMPORARY TABLE temp_patient_lookup;  -- Build once
INSERT INTO fact_encounters
SELECT ... FROM temp_patient_lookup;  -- Use repeatedly
```

**Value**: Reduced ETL time from hours to minutes

### 3. Pre-Aggregated Revenue

**What We Pre-Computed**:
- `total_claim_amount` = SUM(billing.claim_amount)
- `total_allowed_amount` = SUM(billing.allowed_amount)
- `diagnosis_count` = COUNT(diagnoses)
- `procedure_count` = COUNT(procedures)

**Result**: Query 4 ran 2.4x faster by avoiding billing join

**Lesson**: Pre-aggregate when queries would otherwise join transactional tables

### 4. Bridge Tables for Many-to-Many

**Problem**: Encounters have multiple diagnoses and procedures

**Solution**: Separate bridge tables preserve fact grain

**Benefit**:
- fact_encounters stays one-row-per-encounter (accurate counts)
- Detailed analysis joins bridges only when needed
- Pre-aggregated counts avoid bridge joins for simple queries

**Lesson**: Bridge tables worth complexity for healthcare's many-to-many relationships

---

## Part 6: What Didn't Work As Expected

### 1. Simple Aggregations (Query 1)

**Expected**: 10x faster with pre-computed dates
**Actual**: 2x slower due to extra dimension joins

**Why**: At this scale, OLTP's simple 2-table join beats star's 3-table join

**Lesson**: Star schema overhead hurts for simple aggregations on medium datasets

### 2. Self-Joins (Query 3)

**Expected**: Faster with indexed surrogate keys
**Actual**: Equal performance (5.4s vs 5.3s)

**Why**: Self-join cost dominates - schema design is irrelevant

**Lesson**: Some operations are schema-agnostic

### 3. Denormalization Benefits

**Expected**: Huge speedup from avoiding specialty join
**Actual**: Modest benefit - Query 1 still slower overall

**Why**: 
- Denormalized specialty in dim_provider helps individual provider queries
- But aggregate queries still join to dim_specialty for consistency
- Duplication cost (50 bytes × 500 providers = 25KB) is trivial
- Performance gain is marginal at this scale

**Lesson**: Denormalization provides convenience more than speed at medium scale

---

## Part 7: Trade-offs Analysis (Reality-Based)

### What We Gave Up

**1. Write Complexity**
- OLTP insert: Simple INSERT INTO encounters
- Star ETL: 500+ lines of code (lookups, aggregations, batching)
- **Cost**: 40 hours to build, 4 hours/month to maintain

**2. Real-Time Latency**
- OLTP: Data available instantly
- Star: 1-24 hour delay (nightly batch)
- **Cost**: Analytics always behind operational data

**3. Storage Duplication**
- OLTP: 1GB for 500K encounters
- Star: 2GB (fact table + dimensions + bridges)
- **Cost**: Negligible (storage is cheap)

### What We Gained

**1. Modest Performance Improvement (1.25x average)**
- Not the 10-25x claimed in theory
- Meaningful for revenue queries (2.4x)
- Revenue queries are most common business use case

**2. Query Simplicity**
- 2-3 fewer joins per query
- No date functions needed
- Pre-aggregated metrics available

**3. Consistent Business Logic**
- Age grouping defined once in ETL
- Fiscal calendar pre-computed
- Revenue rules enforced during load

**4. Foundation for Scale**
- Architecture ready for 5M+ encounters
- Can add partitioning, aggregate tables
- Investment pays off as data grows

### Was It Worth It?

**At 500K encounters**: Marginal value
- 1.25x improvement doesn't justify 40 hours of development
- Revenue query improvement (2.4x) is meaningful but not transformative
- Complexity added for modest gains

**At 5M+ encounters**: Likely worth it
- Expected 3-5x improvement becomes significant
- OLTP indexes degrade, star schema scales better
- Repeated queries compound time savings

**Conclusion**: Star schema is an **investment in future scale**, not an immediate performance win at medium data volumes.

---

## Part 8: Honest Lessons Learned

### 1. Theory Assumes Scale

**Textbook Claims**: "Star schemas are 10-25x faster"
**Reality Check**: Those benchmarks use 10M-100M rows

**At 500K encounters**:
- Not large enough for OLTP to struggle
- Not small enough to be trivial
- Right in the "gray zone" where benefits are modest

**Lesson**: Always ask "at what scale?" when evaluating architecture claims

### 2. Pre-Aggregation Is The Real Win

**The One Clear Benefit**: Pre-aggregating revenue
- Eliminates billing table join (2.4x speedup)
- Most common business query pattern
- Single biggest value add of star schema

**Lesson**: Focus ETL effort on pre-aggregating metrics that eliminate expensive joins, not on building perfect dimensional models

### 3. Not All Queries Benefit Equally

**Query Results**:
- Simple aggregations: Star slower (overhead exceeds benefit)
- Many-to-many: Star modestly faster (surrogate keys help)
- Self-joins: Equal (schema irrelevant)
- Pre-aggregated metrics: Star significantly faster (real value)

**Lesson**: Star schema is optimized for specific query patterns, not all analytical queries

### 4. Bridge Tables Are Worth It

**Despite Complexity**, bridge tables provided:
- Clean fact grain (one row per encounter)
- Detailed diagnosis/procedure analysis when needed
- Pre-aggregated counts for fast filtering

**Lesson**: For healthcare's many-to-many relationships, bridge tables are essential even if performance gains are modest

### 5. ETL Complexity Is Real

**ETL Code Comparison**:
- OLTP insert: 5 lines
- Star ETL: 500+ lines (lookups, pre-aggregation, batching, validation)

**Maintenance Burden**:
- More failure points (dimension lookups, aggregation logic)
- Complex error handling
- Data quality checks required

**Lesson**: ETL complexity is the hidden cost of star schemas - budget development time accordingly

---

## Part 9: Production Recommendations

### When to Use Star Schema

**Clear Use Cases**:
1. **Repeated Analytical Queries**: Same queries run 100+ times per day
2. **Pre-Aggregatable Metrics**: Revenue, volume, counts can be computed during ETL
3. **Expected Data Growth**: Starting with 500K but expecting 5M+ within 2 years
4. **Dashboard Requirements**: Executive dashboards need consistent, fast queries

**This Healthcare Scenario**: Justified because:
- Revenue queries most common (2.4x improvement)
- Clinical dashboards run repeatedly
- Expected growth to millions of encounters
- Regulatory reporting requires consistent metrics

### When to Stick With OLTP

**Avoid Star Schema When**:
1. **Data Volume Small**: Under 1M facts, OLTP works fine
2. **Query Patterns Unknown**: Ad-hoc exploration doesn't benefit from pre-optimization
3. **Real-Time Required**: Can't wait for nightly batch
4. **Limited Resources**: Don't have 40+ hours for ETL development

### Hybrid Approach (Best of Both Worlds)

**Recommendation**:
1. Keep OLTP for operational queries
2. Build star schema for repeated analytical queries
3. Create aggregate fact tables for monthly/yearly summaries
4. Use materialized views for frequently-joined dimensions

**Example**:
```sql
-- Aggregate fact table for instant monthly dashboards
CREATE TABLE fact_monthly_specialty_revenue AS
SELECT 
    month_year,
    specialty_key,
    SUM(total_allowed_amount) AS total_revenue,
    COUNT(*) AS encounter_count
FROM fact_encounters
GROUP BY month_year, specialty_key;

-- Monthly dashboard queries become instant (no aggregation)
SELECT * FROM fact_monthly_specialty_revenue WHERE month_year = 'May 2024';
```

---

## Part 10: Future Optimizations

### At Current Scale (500K)

**Low-Hanging Fruit**:
1. **Partition fact table** by year/month for faster queries
2. **Add covering indexes** for common dimension combinations
3. **Materialize common joins** (fact + dim_specialty)

### At Larger Scale (5M+)

**Advanced Optimizations**:
1. **Column-store indexes**: For analytics workload
2. **Horizontal partitioning**: Split by date range
3. **Read replicas**: Handle concurrent query load
4. **Aggregate fact tables**: Pre-compute monthly/yearly summaries
5. **OLAP cubes**: For slice-and-dice analysis

---

## Conclusion: Honest Data Warehousing

### The Core Truth

**Star schemas are NOT magic performance bullets.** They are:
- Optimized for specific query patterns (pre-aggregated metrics)
- Most valuable at large scale (5M+ facts)
- An investment in future growth
- Trading write complexity for read performance

### The 1.25x Reality

At 500K encounters, our **1.25x average improvement** is honest data warehouse performance:
- Not the 10-25x claimed in theory
- Meaningful for revenue queries (2.4x)
- Marginal for simple aggregations (0.5x)
- Foundation for future scale (expected 3-5x at 5M)

### Final Thought

This project taught me that **data warehouse design is about trade-offs, not absolutes**. The star schema isn't better or worse than OLTP - it's optimized for different workloads and scales.

For healthcare analytics:
- Revenue query improvement (2.4x) justifies investment
- Pre-aggregation eliminates expensive billing joins
- Architecture ready for expected growth
- But we're honest about mixed results at current scale

**The key lesson**: Always test with realistic data volumes and query patterns. Theory is a starting point, not a guarantee.

---

**Project Completed by**: Carl Nyameakyere Crankson 
**Date**: January 2026  
**Course**: Data Engineering  
**Dataset**: 500K encounters, 100K patients, 500 providers

---