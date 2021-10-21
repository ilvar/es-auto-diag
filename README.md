# Automatically analyse Elasticsearch diagnostic dump

To run:

    pipenv run python3 ./analyze_diag.py ./PATH/TO/DUMP/

Checks list:

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