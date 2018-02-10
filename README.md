# N1QL Benchmark

A CLI to benchmark different Couchbase N1QL queries/indexes

## Usage

Execute profiling session
```sh
./n1qlb -i config.json
```

All flags and options are available with the following command:
```sh
./n1qlb -help
```

### Options

* -c Number of logical CPUs used for the test, default 2
* -i Configuration input file, default ```config.json```
* -o Output csv file, default ```report-execution.csv```
* -p Output txt file with the explain of the queries, default ```explain.json```
* -t Query timeout, default 30s (30 seconds)
* -pause Pause between tests, usefult to allow Couchbase to recover and have a clear separation between different queries. Default 2s (2 seconds)


## Configuration file format

Configuration example
```json
{
	"concurrency": [1,2,5,10],
	"createPrimary": true,
	"repetitions": 5,
	"bucket": "default",
	"cbhost": "localhost",
	"password": "",
	"user": "",
	"indexes": [
		{"name": "someIndex", "definition": ["afield", "bfield"], "dropOnFinish": true}
	],
	"queries":[
		{"name": "Indexed", "query": "select * from default where name='John' and lastName='Doe'", "indexes": [{"name": "byName", "definition": ["name", "lastName"], "dropOnFinish": true}]},
		{"name": "Covered", "query": "select name, lastName from default where name='John' and lastName='Doe'", "indexes": [{"name": "byName", "definition": ["name", "lastName"], "dropOnFinish": true}]},
		{"name": "Without index", "query": "select name, lastName from default where name='John' and lastName='Doe'"}
	]
}
```

- concurrency. Number of concurrent calls/users to test against.
- createPrimaryIndex. Self explanatory.
- repetitions. Number of times that each query will be tested against each concurrency.
- bucket. Couchbase bucket.
- cbhost. Couchbase server.
- user. Couchbase user.
- password. Couchbase password.
- indexes. Group of common indexes for all queries (optional), useful to test covered vs non-covered queries. The ```dropOnFinish``` option will make sure that these indexes are removed when set to true.
- queries. Collection of queries to be tested, each one of them can create its own index (that can be dropped after its own test).