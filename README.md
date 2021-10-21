# Automatically analyse Elasticsearch diagnostic dump

First make dump with [support-diagnostics](https://github.com/elastic/support-diagnostics)

To run:

    pipenv run python3 ./analyze_diag.py ./PATH/TO/DUMP/

## Checks list

* Compressed oops
* Shards count
* Shard size (avg + histo?)
* Cluster state size
* refresh_interval
* High cardinality fields (from fielddata)
* Thread pool rejections
* Shard distribution by doc count
* Shard distribution by size
* Shard distribution per node

## Example output

![Output](/example.png)

## TODO

* Web UI
* Drill down into what specifically indices/sharsds/etc are offenders
