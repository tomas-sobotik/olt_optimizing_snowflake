----------------------------------Preparation----------------------------------------------------

--create a DB for our DB objects
use role sysadmin;
CREATE DATABASE OPTIMIZING_SF;

----------------------------------02 Virtual warehouse configuration-----------------------------

use role SYSADMIN;
--creating single VWH with basic params
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Creating a multicluster warehouse with basic params
CREATE WAREHOUSE IF NOT EXISTS MULTI_COMPUTE_WH WITH
  WAREHOUSE_SIZE = SMALL
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = ECONOMY ---overwritting the default STANDARD policy
  AUTO_SUSPEND = 180
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;


--find out the current value of timeout parameter for compute_wh
describe warehouse compute_wh; --no
show warehouses like 'compute_wh'; --no
show parameters for warehouse compute_wh; --yes

describe warehouse multi_compute_wh; --no
show warehouses like 'multi_compute_wh'; --no
show parameters for warehouse multi_compute_wh; --yes

--modify the parameter for compute_wh
alter warehouse compute_wh set STATEMENT_TIMEOUT_IN_SECONDS = 3600;

--other places where query timeouts could be set

--session
show parameters for session;
alter session set STATEMENT_TIMEOUT_IN_SECONDS = 3600;

--account
show parameters for account;
use role accountadmin;
alter account set STATEMENT_TIMEOUT_IN_SECONDS = 172800;

--different settings per wh, session, account? Snowlfake will enforce the lowest available level: warehouse > session > account

--setting the parameter when creating the warehouse
use role sysadmin;
CREATE OR REPLACE WAREHOUSE COMPUTE_WH2
warehouse_size = 'SMALL'
statement_timeout_in_seconds = 3600;

--resource monitors and their assignment to warehouses with monthly frequency (default value)
use role accountadmin;
CREATE OR REPLACE RESOURCE MONITOR compute_quota
  WITH CREDIT_QUOTA = 50
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY --notifications are sent to accountadmins
           ON 98 PERCENT DO SUSPEND
           ON 100 PERCENT DO SUSPEND_IMMEDIATE;

show resource monitors;

--assigning to warehouse
alter warehouse compute_wh
set resource_monitor = compute_quota;

--check the settings
show warehouses;

------------------------------------------04 Query Plan ------------------------------------------

use role accountadmin;

--in case you do not have sample db available at your account you can create it
-- Create a database from the share
CREATE DATABASE SNOWFLAKE_SAMPLE_DATA
FROM SHARE SFC_SAMPLES.SAMPLE_DATA;

    -- Grant the SYSADMIN role access to the database.
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE SYSADMIN;
use database snowflake_sample_data;

use database snowflake_sample_data;
use schema tpch_sf100;

--run sample query
select
    year(o_orderdate) as year,
    count(*) order_count,
    sum(o_totalprice) total_amount
from
    orders
where
    year(o_orderdate) > 1994
group by
    year
order by
    3 desc;

--checking the query plan programmaticaly
select
    *
from
    table(
        get_query_operator_stats('01b2ef9f-0203-8118-0000-cd55003d70b2')
    );

--more complex query with aggregations, joins - producing more nodes in query plan
select
    c_name,
    c_address,
    n_name,
    r_name,
    count(o_orderkey) order_cnt,
    sum(o_totalprice) total_paid
from
    orders o
join
    customer c
on o.o_custkey = c.c_custkey
join
    nation n
on c.c_nationkey = n.n_nationkey
join
    region r
on r.r_regionkey = n.n_regionkey
where
    year(o_orderdate) = 1994
group by
    c_name,
    c_address,
    n_name,
    r_name;

--create a view and checking the query plan
use role sysadmin;
create or replace view optimizing_sf.public.total_orders_per_customer
as select
    c_name,
    c_address,
    n_name,
    r_name,
    count(o_orderkey) order_cnt,
    sum(o_totalprice) total_paid
from
    snowflake_sample_data.tpch_sf100.orders o
join
    snowflake_sample_data.tpch_sf100.customer c
on o.o_custkey = c.c_custkey
join
    snowflake_sample_data.tpch_sf100.nation n
on c.c_nationkey = n.n_nationkey
join
    snowflake_sample_data.tpch_sf100.region r
on r.r_regionkey = n.n_regionkey
where
    year(o_orderdate) = 1994
group by
    c_name,
    c_address,
    n_name,
    r_name;

--query the view - same profile
select * from optimizing_sf.public.total_orders_per_customer;

--creating another view reading from different view
create or replace view  optimizing_sf.public.total_per_country as
select
    n_name,
    count(order_cnt) order_cnt,
    sum(total_paid) total_paid
from
optimizing_sf.public.total_orders_per_customer
group by
n_name
order by order_cnt desc
;

--query the view to check the profile
select * from optimizing_sf.public.total_per_country;

-------------------------------------05 & 06 Optimization strategies -------------------------

--query showing possibilities of query profile
use schema tpch_sf1000;

--heavy query to generate some spillage on large warehouse
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey
        having
            sum(l_quantity) > 100
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate;

--top N long running queries
SELECT
    query_id,
    query_text,
    total_elapsed_time / 1000 AS query_execution_time_seconds,
    partitions_scanned,
    partitions_total,
    BYTES_SPILLED_TO_LOCAL_STORAGE
FROM
    snowflake.account_usage.query_history Q
WHERE
    warehouse_name = 'COMPUTE_WH'
    AND TO_DATE(Q.start_time) > DATEADD(day, -30, TO_DATE(CURRENT_TIMESTAMP()))
    AND total_elapsed_time > 0 --only get queries that actually used compute
    AND error_code IS NULL
    AND partitions_scanned IS NOT NULL
ORDER BY
    total_elapsed_time desc
LIMIT
    10;

--long table scans
Select
 query_id,
 user_name,
 warehouse_name,
 partitions_scanned,
 partitions_total from snowflake.account_usage.query_history
where
    partitions_total > 0 and
(partitions_scanned / partitions_total) > 0.8;

--spilling
Select
 query_id,
 user_name,
 warehouse_name,
 bytes_spilled_to_local_storage,
 bytes_spilled_to_remote_storage from snowflake.account_usage.query_history
where
 bytes_spilled_to_local_storage > 0 or
 bytes_spilled_to_remote_storage > 0;


 ------------------------------07 table scans and pruning------------------------------------------
 use database snowflake_sample_data;
 use schema tpch_sf100;

 --no pruning even though we used filter
 select * from orders where o_totalprice > 200000;

--adding date column to get some pruning
select * from orders where o_totalprice > 200000 and o_orderdate > to_date('1996-01-01', 'YYYY-MM-DD');

--improve the rung time by add only relevant columns
select o_totalprice, o_orderdate, o_orderpriority from orders where o_totalprice > 200000 and o_orderdate > to_date('1996-01-01', 'YYYY-MM-DD');

use schema tpch_sf1000;
--1.5B records in orders table

--utilizing micropartitions metadata
select min(o_orderkey), max(o_orderkey) from orders; --instant result
select min(o_orderdate), max(o_orderdate) from orders; --instant result
select count(*) from orders;

 -- be careful about dates and pruning. Is there any pruning? could it be better?
 select
    o_totalprice,
    o_orderdate,
    o_orderpriority
 from orders
 where o_totalprice > 200000 and
 to_char(o_orderdate, 'YYYY-MM-DD') > '1996-01-01'; --40s

--improving the previous query - can you tell me what is the difference?
select
    o_totalprice,
    o_orderdate,
    o_orderpriority
from orders
where o_totalprice > 200000 and
o_orderdate > to_date('1996-01-01', 'YYYY-MM-DD');
-- 25s -> 38% faster!, pruned partitions were doubled!

 --pruning works on expressions as well
select o_totalprice, o_orderdate, o_orderpriority from orders where o_totalprice > 200000 and DATE_TRUNC('MONTH', o_orderdate) = to_date('1996-01-01', 'YYYY-MM-DD');


 --join filter - have a look how optimizer can filter the base tables automatically
use schema tpcds_sf10tcl;

--testing query
select
   c.c_last_name, count(*) order_cnt
 from
   customer c
 join
   store_sales s
 on c.c_customer_sk = s.ss_customer_sk
 join
   date_dim d
 on s.ss_sold_date_sk = d.d_date_sk
 where
   d.d_year = 1999
 group by
   c_last_name;

---------------------------------08 Clustering ----------------------------------------------------
--check the clustering info by using the functions
use database snowflake_sample_data;

use schema tpch_sf100;

--checking clustering info in orders table
select SYSTEM$CLUSTERING_INFORMATION('orders'); --150M records

select SYSTEM$CLUSTERING_INFORMATION('orders', '(o_custkey)'); --bad column

select SYSTEM$CLUSTERING_INFORMATION('orders', '(o_orderdate)'); --high overlap & depth



use schema tpch_sf1000;
select SYSTEM$CLUSTERING_INFORMATION('orders'); -- we can omit the column parameter

--testing adding more columns into clustering key
select SYSTEM$CLUSTERING_INFORMATION('orders', '(o_orderdate, o_orderstatus)');

--getting clustering depth
select SYSTEM$CLUSTERING_DEPTH('orders');

--high cardinality column
select SYSTEM$CLUSTERING_DEPTH('orders', '(o_custkey)'); --useless clustering

--checking a cost
select * from snowflake.account_usage.AUTOMATIC_CLUSTERING_HISTORY;


--natural clustering
use schema tpch_sf100;

--selecting data along "natural cluster key"
select * from orders where o_orderdate between to_date('01/05/1996', 'DD/MM/YYYY') and to_date('05/05/1996', 'DD/MM/YYYY'); --3.4s

--creating table with ordered data
create or replace table optimizing_sf.public.orders_clustered as
select * from orders order by o_orderdate;

--natural clustering test
select * from optimizing_sf.public.orders_clustered where o_orderdate between to_date('01/05/1996', 'DD/MM/YYYY') and to_date('05/05/1996', 'DD/MM/YYYY'); --2.3s --roughly 30% improvement!

--disable cache for testing purposes
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

--more advanced query
select
    c_name,
    c_address,
    n_name,
    sum(o_totalprice) total_value,
    count(o_orderkey) orders_cnt
from
    orders o
join
    customer c on o.o_custkey = c.c_custkey
join
    nation n on c.c_nationkey = n.n_nationkey
where
    o.o_orderdate between to_date('01/05/1996', 'DD/MM/YYYY') and to_date('31/05/1996', 'DD/MM/YYYY')
group by
    c_name, c_address, n_name
having
    count(o_orderkey) > 1
order by orders_cnt desc
; --3.7s

--querying the table with our natural clustering
select
    c_name,
    c_address,
    n_name,
    sum(o_totalprice) total_value,
    count(o_orderkey) orders_cnt
from
    optimizing_sf.public.orders_clustered o
join
    customer c on o.o_custkey = c.c_custkey
join
    nation n on c.c_nationkey = n.n_nationkey
where
    o.o_orderdate between to_date('01/05/1996', 'DD/MM/YYYY') and to_date('31/05/1996', 'DD/MM/YYYY')
group by
    c_name, c_address, n_name
having
    count(o_orderkey) > 1
order by
    orders_cnt desc
; --2.1s -> 44% improvement:

--enable cache again
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

--bad clustering example
/*

| {                                                                  |
|   "cluster_by_keys" : "LINEAR(ID, NAME)",                        |
|   "total_partition_count" : 1156,                                  |
|   "total_constant_partition_count" : 0,                            |
|   "average_overlaps" : 117.5484,                                   |
|   "average_depth" : 64.0701,                                       |
|   "partition_depth_histogram" : {                                  |
|     "00000" : 0,                                                   |
|     "00001" : 0,                                                   |
|     "00002" : 3,                                                   |
|     "00003" : 3,                                                   |
|     "00004" : 4,                                                   |
|     "00005" : 6,                                                   |
|     "00006" : 3,                                                   |
|     "00007" : 5,                                                   |
|     "00008" : 10,                                                  |
|     "00009" : 5,                                                   |
|     "00010" : 7,                                                   |
|     "00011" : 6,                                                   |
|     "00012" : 8,                                                   |
|     "00013" : 8,                                                   |
|     "00014" : 9,                                                   |
|     "00015" : 8,                                                   |
|     "00016" : 6,                                                   |
|     "00032" : 98,                                                  |
|     "00064" : 269,                                                 |
|     "00128" : 698                                                  |
|   },


*/


---------------------------------10 Optimizing queries --------------------------------------------

--WHERE clause
select * from snowflake_sample_data.tpch_sf1000.orders
where to_char(o_orderdate, 'DD-MM-YYYY') = '06-11-1994'; --runtime 42s

select * from snowflake_sample_data.tpch_sf1000.orders
where o_orderdate = to_date('06-11-1994','DD-MM-YYYY'); --3s

select count(distinct o_orderkey) from snowflake_sample_data.tpch_sf1000.orders; --1m 27s

select approx_count_distinct(o_orderkey) from snowflake_sample_data.tpch_sf1000.orders; --19s
--1491111415


--join order, 40s
select
    c_name,
    c_address,
    n_name
from
    snowflake_sample_data.tpch_sf1000.customer c
left join
    snowflake_sample_data.tpch_sf1000.nation n
on c.c_nationkey = n.n_nationkey;

--4s, right handed tree example
select
    c_name,
    c_address,
    n_name,
    r_regionkey,
    o_orderdate,
    o_totalprice
from
    snowflake_sample_data.tpch_sf1000.customer c
left join
    snowflake_sample_data.tpch_sf1000.nation n
on c.c_nationkey = n.n_nationkey
left join
    snowflake_sample_data.tpch_sf1000.region r
on n.n_regionkey = r.r_regionkey
left join
    snowflake_sample_data.tpch_sf1000.orders o
on c.c_custkey = o.o_custkey
where
    o_orderdate = to_date('06-11-1994','DD-MM-YYYY')
;

--CTEs
--single call
with all_suppliers as (
    select *
    from snowflake_sample_data.tpch_sf1000.supplier
)
select * from all_suppliers;

--multiple call of CTE - pushing the filter down to tablescan
with all_suppliers as (
    select *
    from snowflake_sample_data.tpch_sf1000.supplier
),

argentina_suppliers as (
    select *
    from all_suppliers
    where s_nationkey = 1
),

brazil_suppliers as (
    select *
    from all_suppliers
    where s_nationkey = 2
)

select
    *
from argentina_suppliers
union all
select
    *
from brazil_suppliers;

--replacing CTE name with table name, two table scans, only two filters
with argentina_suppliers as (
    select *
    from snowflake_sample_data.tpch_sf1000.supplier
    where s_nationkey = 1
),

brazil_suppliers as (
    select *
    from snowflake_sample_data.tpch_sf1000.supplier
    where s_nationkey = 2
)

select *
from argentina_suppliers
union all
select *
from brazil_suppliers;


--column pruning - no pruning
with all_suppliers as (
    select *
    from snowflake_sample_data.tpch_sf1000.supplier
),

supplier_address as (
    select s_address
    from all_suppliers

),

supplier_phone as (
    select s_phone
    from all_suppliers

)

select s_address
from supplier_address
union all
select s_phone
from supplier_phone;

--column pruning achieved - replacing CTEs with table names
with supplier_address as (
    select s_address
    from snowflake_sample_data.tpch_sf1000.supplier
),

supplier_phone as (
    select s_phone
    from snowflake_sample_data.tpch_sf1000.supplier
)

select s_address
from supplier_address
union all
select s_phone
from supplier_phone;

/*
Key takeaways:
 - calculating CTE multiple times uses cached result - might be better until some level of complexity
 - when CTE is complex enough it's cheaper to calculate CTE once and then reuse it
 - not stable behaviour - need to experiment
 - column pruning works when CTE is referenced once

*/

-------------------------------------------- merge statement ------------------------------------------------
use database snowflake_sample_data;
use schema tpch_sf100;
use role sysadmin;


--simple merge to explain query profile
CREATE OR REPLACE TABLE optimizing_sf.public.items (
 id NUMBER,
 is_available BOOLEAN,
 updated_date DATE
)
;

CREATE OR REPLACE TABLE optimizing_sf.public.source_items (
 id NUMBER,
 is_available BOOLEAN
)
;

-- Inserting test values
INSERT INTO optimizing_sf.public.items VALUES
(1, TRUE, '2024-02-20'),
(2, TRUE, '2024-02-21'),
(3, FALSE, '2024-02-22')
;

INSERT INTO optimizing_sf.public.source_items VALUES
(1, FALSE), -- update record
(2, TRUE), -- update record
(4, TRUE) -- new record
;


-- Insert a missing row and update the existing ones
MERGE INTO optimizing_sf.public.items tgt
USING optimizing_sf.public.source_items src
 ON tgt.id = src.id
WHEN MATCHED THEN
 UPDATE SET
   tgt.is_available = src.is_available,
   tgt.updated_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
 INSERT
   (id, is_available, updated_date)
 VALUES
 (src.id, src.is_available, current_timestamp())
;


--Dynamic pruning example - improving merge statement performance
-- update of single record means update the whole micropartition

--creating target table - 600m rows
create or replace table optimizing_sf.public.lineitem as
select * from snowflake_sample_data.tpch_sf100.lineitem
order by l_commitdate;

--updating just two records
create or replace table optimizing_sf.public.lines as (
 select
    83010182 as l_orderkey,
    5 l_linenumber,
    to_date('1992-09-05', 'YYYY-MM-DD') l_commitdate,
    'test' l_comment
 union all
 select
   112564066 as l_orderkey,
    4 l_linenumber,
    to_date('1992-09-05', 'YYYY-MM-DD') l_commitdate,
    'test 2' l_comment
);

--merge
merge into optimizing_sf.public.lineitem as target
using data_engineering.public.lines source
on
  target.l_orderkey = source.l_orderkey and
  target.l_linenumber = source.l_linenumber
when matched then update set
    target.l_comment = source.l_comment
;

--now trying to add dynamic pruning

--recreating clone
create or replace table optimizing_sf.public.line_item_clone clone optimizing_sf.public.lineitem;

--merge with pruning column = scanning only relevant partitions
merge into optimizing_sf.public.line_item_clone as target
using optimizing_sf.public.lines source
on
  target.l_orderkey = source.l_orderkey and
  target.l_linenumber = source.l_linenumber and
  target.l_commitdate = source.l_commitdate --pruning column
when matched then update set
    target.l_comment = source.l_comment
;
