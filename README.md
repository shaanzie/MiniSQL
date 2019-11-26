# MiniSQL

This project is a basic implementation of Apache Hive running on top of Apache Hadoop MapReduce. The project deals mainly with basic queries such as load, select and delete. 

## Usage
This project mainly deals with the MapReduce Python API, and this can be found in MiniSQL/PyImpl. The syntax for running queries is given below. Please ensure that Hadoop is running on localhost:9000 and ensure node write permissions are enabled for the folder from where the code is being run.

### Load
`load bigdata/test.csv as [column1:int,column2:float,column2:string];`

### Select
`select * from bigdata/test.csv where column1 > 30;`

### Delete
`delete bigdata/test.csv;`
