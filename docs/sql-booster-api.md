## How to use sql-booster.mlsql.tech

This tutorial will show you how to use APIs in sql-booster.mlsql.tech.

## APIs 

APIs help information:

```
GET/POST http://sql-booster.mlsql.tech/api
```

Table Register:

```
POST http://sql-booster.mlsql.tech/api_v1/table/register
```

View Register:

```
POST  http://sql-booster.mlsql.tech/api_v1/view/register
```

Data Lineage Analysis:

```
POST http://sql-booster.mlsql.tech/api_v1/dataLineage
```

View Based SQL Rewriting:

```
POST  http://sql-booster.mlsql.tech/api_v1/mv/rewrite
```

## Summary

The design and behavior of sql-booster allows you to analyse or rewrite SQL without real table exits. 
The only thing you should do is register table/view create statement before using functions like Lineage Analysis 
and View Based SQL Rewriting. 

Also, you should identify who invoke the API, and the system will create a uniq session for you. This will make your tables
registered will not mess up with the other's. The example bellow I will use name `allwefantasy@gmail.com` to identify my
requests. 

We strongly recommend you using PostMan to play with this APIs since most of them only support POST method. 


## Register tables

We will register three tables firstly, and you can use PostMan to post follow data 
to  `http://sql-booster.mlsql.tech/api_v1/table/register`.


table depts:

```
name:allwefantasy@gmail.com
tableName:depts
schema:CREATE TABLE depts(↵  deptno INT NOT NULL,↵  deptname VARCHAR(20),↵  PRIMARY KEY (deptno)↵);
```

table locations:

```
name:allwefantasy@gmail.com
tableName:locations
schema:CREATE TABLE locations(↵  locationid INT NOT NULL,↵  state CHAR(2),↵  PRIMARY KEY (locationid)↵);
```

table emps:

```
name:allwefantasy@gmail.com
tableName:emps
schema:CREATE TABLE emps(↵  empid INT NOT NULL,↵  deptno INT NOT NULL,↵  locationid INT NOT NULL,↵  empname VARCHAR(20) NOT NULL,↵  salary DECIMAL (18, 2),↵  PRIMARY KEY (empid),↵  FOREIGN KEY (deptno) REFERENCES depts(deptno),↵  FOREIGN KEY (locationid) REFERENCES locations(locationid)↵);
```

## Data Lineage

Visit `http://sql-booster.mlsql.tech/api_v1/dataLineage` to analyse data lineage for any SQL:

```
sql:select * from (SELECT e.empid↵FROM emps e↵JOIN depts d↵ON e.deptno = d.deptno↵where e.empid=1) as a where a.empid=2
name:allwefantasy@gmail.com
```

The response looks like this:

```json
{
    "outputMapToSourceTable": [
        {
            "name": "empid",
            "sources": [
                {
                    "tableName": "emps",
                    "columns": [
                        "empid"
                    ],
                    "locates": [
                        [
                            "PROJECT",
                            "FILTER"
                        ]
                    ]
                },
                {
                    "tableName": "depts",
                    "columns": [],
                    "locates": []
                }
            ]
        }
    ],
    "dependences": [
        {
            "tableName": "emps",
            "columns": [
                "empid",
                "deptno"
            ],
            "locates": [
                [
                    "PROJECT",
                    "FILTER"
                ],
                [
                    "JOIN"
                ]
            ]
        },
        {
            "tableName": "depts",
            "columns": [
                "deptno"
            ],
            "locates": [
                [
                    "JOIN"
                ]
            ]
        }
    ]
}
```

## View Based SQL Rewrite

Register View with API `http://sql-booster.mlsql.tech/api_v1/view/register`

```
viewName:emps_mv
name:allwefantasy@gmail.com
sql:SELECT empid↵FROM emps↵JOIN depts ON depts.deptno = emps.deptno
```

Sending a SQL to `http://sql-booster.mlsql.tech/api_v1/mv/rewrite`:

```
name:allwefantasy@gmail.com
sql:select * from (SELECT e.empid↵FROM emps e↵JOIN depts d↵ON e.deptno = d.deptno↵where e.empid=1) as a where a.empid=2
```

The response looks like follow:

```sql
SELECT a.`empid`
FROM (
	SELECT `empid`
	FROM emps_mv
	WHERE `empid` = CAST(1 AS BIGINT)
) a
WHERE a.`empid` = CAST(2 AS BIGINT)
```

Notice that we have replaced emps,depts by the view emps_mv we have created before. 




