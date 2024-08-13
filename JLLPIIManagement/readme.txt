basic information and guidance: 
1. .properties files like dataSource.properties contain Database JDBC connection information.
2. pii_data_default.xml is the main file contains all of the data pii database table columns.



technical items need to be improved:
1. The program is only for sqlserver and oracle, have not finished the implement for mysql, DB2, sybase.....
2. We need to check all possible data types for the target data columns, currently we only work on char, varchar, decimal, integer, smallint, double, Date. we have not tested to protect Time or Timestamp data types.
3. We need to think of the approach how to recover, re-mask, decrypt the data if needed.
4. have not applied literal String in the where clause, like "O'Sullivan".
5. the current code is not including the schema name, like "afm" or "dbo". this logic needs to be added.
6. It may cause the duplicate key violation after we protect the data. for example: there are 2 employees called Jane,Brown and Jack,Brown, if the employee name is the key in the table and we mask the first 4 characters of the employee names, they will both get xxxx,Brown which will lead to the violation.


Please provide me any of your advice on improving this program:
qian.han@am.jll.com