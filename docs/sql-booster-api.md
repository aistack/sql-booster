## How to use sql-booster.mlsql.tech

This tutorial will show you how to use APIs in sql-booster.mlsql.tech 

## Register tables

Using PostMan to post follow data to  `http://sql-booster.mlsql.tech/api_v1/table/register`.
We will register three tables:

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

Visit `http://sql-booster.mlsql.tech/api_v1/dataLineage` to do data lineage analysis

```
sql:select * from (SELECT e.empid↵FROM emps e↵JOIN depts d↵ON e.deptno = d.deptno↵where e.empid=1) as a where a.empid=2
name:allwefantasy@gmail.com
```

The response:

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

Register View with API http://sql-booster.mlsql.tech/api_v1/view/register

```
viewName:emps_mv
name:allwefantasy@gmail.com
sql:SELECT empid↵FROM emps↵JOIN depts ON depts.deptno = emps.deptno
```

Do rewriting with API http://sql-booster.mlsql.tech/api_v1/mv/rewrite

```
name:allwefantasy@gmail.com
sql:select * from (SELECT e.empid↵FROM emps e↵JOIN depts d↵ON e.deptno = d.deptno↵where e.empid=1) as a where a.empid=2
```

The response:

```sql
SELECT a.`empid`
FROM (
	SELECT `empid`
	FROM viewName
	WHERE `empid` = CAST(1 AS BIGINT)
) a
WHERE a.`empid` = CAST(2 AS BIGINT)
```




