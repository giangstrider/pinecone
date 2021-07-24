# Pinecone

Pinecone is a Data Reconciliation application build on Scala to reconciling data between multiple databases with various reporting and monitoring capabilities. It comes up with a configurable approach and different deployment strategies fit for many data teams to operating reconciliation workloads efficiently.
<br /><br />

# Motivation

Data mismatch has always been a headache problem for data engineer teams who maintain multiple data pineline workloads sourcing from several databases ranging from back-end application team to ETL pipeline from persistent stage to Data mart. Data stakeholders come to the team and say there is a data mismatch or at least their suspectation. The team needs to manually run SQL queries against data source, and then data destination. Sometimes, it is a historical data which has nothing wrong in the past but suddenly mismatch, or you just run a job, finish the pineline and want to make sure that data looks OK. These kinds of works happen all the time and eventually become a "new official" job description since the demand of moving data from one place to multiple places growing gradually.

Pinecone aims to solve the problem by basic idea that form up SQL queries run against source and target data. It provides a set of rules for users to set up their expectation on accepted deviation and how do you want to compare the results. It also provides `Pinecone variable` which is the variable template that you can use in your own SQL queries and it will be transformed to the value at run-time. Finally, set your own rules by defining how to monitor it and how to make changes with ease, integrating with modern orchestration workflow such as Airflow to operate in data engineering fashion.
<br />
<br />

# Note

This project developed internally. I'm rewriting it in a general approach to release as an open source project.
To be released soon.
All contributions are more than welcome.
Please reach out to me on email strider.giang@live.com or Github issues.  
<br />

# Main Features
 
- Define your own reconciliation workflow basically including data source and data target with many options for reporting, monitoring, and alert notification.
- Connect to multiple database connections via JDBC to execute your SQL queries.
- SQL templating: using Pinecone's define variable to dynamically replace itself value at run time (such as \${yesterday}, ${last_6_months} ...)
- Numeric reconciliation and String like reconciliation.
- Rolling back your entire's workflow definition to any specific point of time.
 
# Core Concepts
The basic idea of Pinecone is the central place for executing a set of reconciliation queries against many data sources and compare them with your own definition and decide what to do with the compared result. This is called Reconciliation Workflow.
 
It also has capability to configure your queries run back at any point of time, compare and generate the report around it. But again, this is based on the concept of Reconciliation Workflow.
 
By utilizing JDBC, we can connect to almost any database available. Not only databases, it also means that we can also utilize the computing capacity of many query execution engines such as Trino or Spark.
 
## What's Pinecone compare? How does the comparison work?
 
### General comparison
The comparison between the results of two SQL queries operate on a data equally check basis.
Basically, Pinecone compares two tables (returned by SQL queries) and figures out the differences between them.
 
But since data volume of many companies extremely large, it is recommended that you should avoid the concept of compare directly row by row - (even though Pinecone still support it)
 
Famous example is
```sql
SELECT * FROM TABLE
```
 
Being said that, Pinecone is the tool that operates on your aggregated level of data metrics. For example, instead `SELECT *`, the way it should is
 
```sql
SELECT INSERT_DATE, COUNT(*) FROM TABLE GROUP BY INSERT_DATE
--or
SELECT COUNT(*) FROM TABLE
```
The metrics in this context is something already similar to any data professional such as `COUNT`, `SUM`, `MAX/MIN` ... which operate together with a `GROUP BY` statement. The goal of Pinecone is about to operate on a high level of data table, rather than go down row-by-row.
 
### Reconcile key
Consider this example:
```sql
SELECT DEPARTMENT, COUNT(*) FROM TABLE GROUP BY DEPARTMENT
--or previous example
SELECT INSERT_DATE, COUNT(*) FROM TABLE GROUP BY INSERT_DATE
```
 
`DEPARTMENT` and `INSERT_DATE` is a grouped column to do the `COUNT` function. As mentioned above, Pinecone operate on high level of data, concept of `Reconcile key` is to specify your grouped columns in order to:
- Know which columns should be operate the [deviate reconciliation](#deviate-reconciliation).
- Determine whether on the time when workflow is executed, the grouped columns are missing compared to other sources. This is also a key optimization internally where Pinecone compares a large volume of data.
 
`Reconcile key` is an important concept which helps Pinecone operate efficiently.
 
### Metadata check
Not only data level, Pinecone is also able to check metadata for each column, and decide whether type of column is matched or not. Also rate the comparable level between columns of two systems in case it is not matched.

```sql

-- Database A
SELECT AMOUNT FROM TABLE -- Return 5
-- Database B
SELECT AMOUNT FROM TABLE -- Return 5.0
```

As you can see in above example, data type on `AMOUNT` column high chance is `INT`, meanwhile B maybe a `DOUBLE`. 

This could be major issues in term of data quality. Even though data in both table when you doing a `SUM` maybe the same. You can't know one day, a `DOUBLE` could be `5.5` meanwhile A not able to represent the precision.

Another example in Snowflake database:
```
-- Database A
SELECT AMOUNT FROM TABLE -- Type of Amount: NUMBER(23, 10)
-- Database B
SELECT AMOUNT FROM TABLE -- type of Amount: NUMBER(23, 5)
```
This precision could be a potenial bug in future.

By utilizing JDBC metadata featur, Pinecone add this capability to enhance data reconciliation process.
### SQL Templating
 
In order to reconcile effectively, Pinecone came up with the `SQL Templating` feature that can replace dynamically variables in your SQL queries at runtime.
 
 
Imagine you want to set up periodically for a workflow running on time basis, you can define Pinecone's SQL syntax and Pine will do the job.
 
An example you can think of is dynamically change value of `$PINECONE{MONTH(-1)}`:
 
```sql
SELECT TRANSACTION_DATE, COUNT(*) FROM TABLE WHERE MONTH(TRANSACTION_DATE) = $PINECONE{MONTH(-1)}
--$PINECONE{MONTH(-1)} ON 15-06-2021 will be 5
```
 
As you may think, you can just use the database's function to do that. This is correct. But this feature aim to:
- Unify syntax across the workflows. It may take time to explore syntax for each database and it also is hard to understand when you have too many syntax for the same utility. And when unified using Pinecone's feature, you can just make one change and apply for all of the workflows.
- By unified syntax, Pinecone is also able to rolling all workflows to any point of time when your workflow is just over a hundred, it is pain to apply manually for each workflow.


<br />

# Table of Contents
1. [Configuration](#configuration)
- [Database connection](#database-connection)
- [Reconciliation workflow](#reconciliation-workflow)
- Application's configuration
2. [SQL query construction](#example2)
- Basic requirement
- SQL Templating
3. [Deployment](#third-example)
- Basic requirement
- Deploy
    - Docker
    - CLI


# Configuration
This is the most important part in order to set up connections, workflows and other attributes. It also explain how the application works.

Configuration utilizes [pureconfig](https://github.com/pureconfig/pureconfig) library. While the details here are enough for you to set up your own, please refer to that library for any clarifications.

Many sections below come up with popular syntax which is used here.
- `${ENVIRONMENT_VARIABLE_VALUE}`: Avoid exposing sensitive data as well as apply correct value for each environment. This variable will depend on how you deploy Pinecone, [see deployment section](#deployment).
## Database Connection

Only support databases with supported JDBC drivers. Theoretically any databases which supported JDBC will be worked natively with Pinecone, since underlying Pinecone uses raw JDBC syntax support for JVM languages.

Here is the list of supported databases which fully tested with Pinecone, some of them utilize extra feature from database's JDBC driver (noted beside it).

- SQL Server
- MySQL
- PostgreSQL
- IBM DB2
- Snowflake ([with async support](#snowflake-async-support))

Upcoming support:
- AWS Redshift

All connections will be define in `connection.conf`.

### Connection Syntax

As mentioned, the syntax is supported by [pureconfig](https://github.com/pureconfig/pureconfig) library. For database connection, it also utilizes `connection key` supported by [HikariCP](https://github.com/brettwooldridge/HikariCP).

Basic syntax:
```
jdbc {
    connection-name = {
        database-connection-key = database-connection-value
    }
    // 2nd connection
    your-second-connection-name = {
        database-connection-key = database-connection-value
    }
 }
```

| Attribute          | Definition                                                                                                                                                                                                                                                                                                                                                                                                                       |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connection-name    | Name of your data source. E.g: Sales team, Operation Tesla team, Marketing Data mart... **This connection name will be used in reconciliation workflow**.                                                                                                                                                                                                                                                                        |
| connection-key     | Attribute name [supported by HikariCP](https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby).<br><br>    While most of attributes are optional, few attributes must be present:<br>        - `jdbcUrl`: JDBC Connection string<br>        - `username`: database's user. Highly recommend using Environment variable here.<br>        - `password`: database's password.<br><br>    Optional useful attributes:<br>        - `connectionInitSql`: the query you want to run in database's session. Eg: Common example would be `ALTER SESSION SET TIMEZONE='UTC'` (to synchronize time zone across connections). |

**Note**: together with data source connection, Pinecone's metadata database also defined here.

The final example will look like:
```
jdbc {
    pinecone = {
        jdbcUrl = "jdbc:snowflake://pinecone.ap-southeast-2.snowflakecomputing.com/?warehouse=recon&db=prod&role=admin"
        username = ${PINECONE_USER}
        password = ${PINECONE_PASSWORD}
        connectionInitSql = "ALTER SESSION SET TIMEZONE='UTC'"
    }
    marketing-mart = {
        jdbcUrl = "jdbc:postgresql://host/database
        username = ${MARKETING_USER}
        password = ${MARKETING_PWD}
    }
 }
```

<br />

## Reconciliation workflow

The basic workflow will include data source and target (data destination). Define your SQL query for each data connection, as well as many attributes such as accepted deviation, when Pinecone should send notification to data stakeholders regarding data mismatch, or define your own reporting template.

Workflow defined under `queries.conf`.
This is the core features of Pinecone so it also has many attributes need to focus on.

### Syntax
```
queries = [
    {
        query-key = query-key-value
        source-name = connection-name
        source-query = {type = "file-base", sql-file = "file-location"}
        target-name = nection-name
        target-query = {type = "text-base", sql-query = "sql-query-string"}
        reconcileKey = ["TradingDate"]
        accepted-deviation = double-value
    }
```

| Attribute          | Definition                                                                                                                                                                                                                                                                                                                                                                                                                       |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| query-key          | Unique name across your workflows. Pinecone considers it as a "primary key" across application's logic. <br>If you come up with same workflow but run against different data sources, it is recommended that put the data source name along `query-key` it self.                                                                                                                                                                  |
| source-name        | Your database connection name defined in [database connection section](#connection-syntax).<br>This attribute acts as a Data Source.                                                                                                                                                                                                                                                                                                                    |
| source-query       | Your SQL string will be received here.<br>You can simply put for SQL String, or if it is too long and better to be a separate file (to utilize IDE's syntax highlighting for example), Pinecone also support it.<br><br>- For SQL String: `{type = "text-base", sql-query = "SELECT CURRENT_DATE - 1 AS OPERATION_DATE FROM COSTS GROUP BY OPERATION_DATE"}`<br>- For SQL File: `{type = "file-base", sql-file = "/unix-path/file.sql"}   `                                                                                                                                                           |
| target-name        | Same with `source-name`. Except this acts as a Data Target.                                                                                                                                                                                                                                                                                                                                                                       |
| target-query       | Same with `source-query`. Except this acts as a Data Target.                                                                                                                                                                                                                                                                                                                                                                      |
| reconcile-key      | This attribute explained in details in SQL [requirement section](#sql-basic-requirement).<br><br>Define it as an "array" or "list" in other languages. Eg: ["INSERT_DATE", "USER_GROUP"]                                                                                                                                                                                                                                         |
| accepted-deviation | The maximum deviation to be accepted. <br>Any number greater than this will consider as a data mismatch and will raise notification to stakeholders if there is a config for it.<br>See [Monitoring section](#monitoring).<br><br>Default: `1` (percent)<br><br>Depends on your data ingestion pattern, there will be a small deviation between two systems. <br>It is recommended that workflow should be scheduled to reconcile yesterday's data, so you can set it to `0`. |

Example config:
```
queries = [
    {
        query-key = "click_daily"
        source-name = "marketing-mart"
        source-query = {type = "text-base", sql-query = "SELECT CURRENT_DATE - 1 AS OPERATION_DATE FROM CLICKS GROUP BY OPERATION_DATE"}
        target-name = "central-mart"
        target-query = {type = "file-base", sql-file = "/SQL/click/clickstream.sql"}
        reconcileKey = ["OPERATION_DATE"]
        accepted-deviation = 1
    }
    {
        query-key = "cost_daily"
        source-name = "marketing-mart"
        source-query = {type = "text-base", sql-query = "SELECT CURRENT_DATE - 1 AS OPERATION_DATE FROM COSTS GROUP BY OPERATION_DATE"}
        target-name = "central-mart"
        target-query = {type = "file-base", sql-file = "/SQL/click/cost.sql"}
        reconcileKey = ["OPERATION_DATE"]
        accepted-deviation = 0
    }
```