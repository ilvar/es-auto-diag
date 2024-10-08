#/usr/bin/env python3

import json
import sys
import os
import re

import rich.console
import rich.table

import plotille


class Result():
    CODE_CLUSTER_HEALTH = "CLUSTER_HEALTH"
    CODE_COMPRESSED_OOPS = "COMPRESSED_OOPS"
    CODE_OVERSHARDING = "OVERSHARDING"
    CODE_MANY_SMALL_SHARDS = "MANY_SMALL_SHARDS"
    CODE_MANY_LARGE_SHARDS = "MANY_LARGE_SHARDS"
    CODE_CLUSTER_STATE_SIZE = "CLUSTER_STATE_SIZE"
    CODE_REFRESH_INTERVAL = "REFRESH_INTERVAL"
    CODE_THREAD_POOL_REJECTIONS = "THREAD_POOL_REJECTIONS"
    CODE_HOT_THREADS = "HOT_THREADS"
    CODE_DOCS_COUNT = "DOCS_COUNT"
    CODE_DURATION = "DURATION"
    CODE_GC = "GC"

    def __init__(self, message, code, bad=False, value=None) -> None:
        self.message = message
        self.code = code
        self.value = value
        self.bad = bad

    def get_message(self):
        return self.message

    def get_code(self):
        return self.code

    def get_value(self):
        return self.value

    def is_bad(self):
        return self.bad

    def is_good(self):
        return not self.bad

    def to_dict(self):
        return {
            "message": self.message,
            "code": self.code,
            "value": self.value,
            "bad": self.bad,
        }
    
    @staticmethod
    def from_dict(data):
        return Result(
            message=data["message"],
            code=data["code"],
            value=data["value"],
            bad=data["bad"],
        )

class Analyzer():
    root_path = None
    console = None

    results = []
    charts = []
    
    GB = 1024 * 1024 * 1024

    def __init__(self, root_path: str):
        self.root_path = root_path
        self.console = rich.console.Console()

    def _load_json(self, fname: str) -> any: 
        with open(os.path.join(self.root_path, fname)) as f: 
            return json.load(f)

    def check_cluster_health(self):
        cluster_health = self._load_json("cluster_health.json")

        if cluster_health["status"] != "green":
            self.results.append(Result(
                "Cluster is: %s" % cluster_health["status"].upper(),
                code=Result.CODE_CLUSTER_HEALTH,
                bad=True,
                value=cluster_health["status"],
            ))
        else:
            self.results.append(Result(
                "Cluster is: GREEN",
                code=Result.CODE_CLUSTER_HEALTH,
                bad=False,
                value=cluster_health["status"],
            ))    

    def check_nodes(self):
        nodes_data = self._load_json("nodes.json")["nodes"]
        node_count = len(nodes_data)
        compressed_oops_count = sum([n["jvm"]["using_compressed_ordinary_object_pointers"] == "true" for n in nodes_data.values()])

        if compressed_oops_count < node_count:
            self.results.append(Result(
                "Compressed OOPs off for %s nodes out of %s" % (node_count - compressed_oops_count, node_count),
                code=Result.CODE_COMPRESSED_OOPS,
                bad=True,
                value=node_count - compressed_oops_count,
            ))
        else:
            self.results.append(Result(
                "Compressed OOPs on for all nodes",
                code=Result.CODE_COMPRESSED_OOPS,
                bad=False,
            ))

    def check_indices(self):
        indices_data = self._load_json("indices_stats.json")

        total_docs = indices_data["_all"]["primaries"]["docs"]["count"]
        deleted_docs = indices_data["_all"]["primaries"]["docs"]["deleted"]

        values = (total_docs, deleted_docs, deleted_docs / total_docs * 100)
        self.results.append(Result(
            "Total docs: %s; deleted docs: %s (%.2f%%)" % values,
            code=Result.CODE_DOCS_COUNT,
            bad=False,
            value=values,
        ))

        refresh_duration_millis = indices_data["_all"]["primaries"]["refresh"]["total_time_in_millis"]
        refresh_duration_hours = refresh_duration_millis / 1000 / 3600

        self.results.append(Result(
            "Refresh duration: total %.2f hours" % refresh_duration_hours,
            code=Result.CODE_DURATION,
            bad=False,
            value=refresh_duration_hours,
        ))

        flush_duration_millis = indices_data["_all"]["primaries"]["flush"]["total_time_in_millis"]
        flush_duration_hours = flush_duration_millis / 1000 / 3600

        self.results.append(Result(
            "Flush duration: total %.2f hours" % flush_duration_hours,
            code=Result.CODE_DURATION,
            bad=False,
            value=flush_duration_hours,
        ))

        index_duration_millis = indices_data["_all"]["primaries"]["indexing"]["index_time_in_millis"]
        index_duration_hours = index_duration_millis / 1000 / 3600

        self.results.append(Result(
            "Indexing duration: total %.2f hours" % index_duration_hours,
            code=Result.CODE_DURATION,
            bad=False,
            value=index_duration_hours,
        ))

        search_duration_millis = indices_data["_all"]["primaries"]["search"]["query_time_in_millis"]
        search_duration_hours = search_duration_millis / 1000 / 3600

        self.results.append(Result(
            "Search duration: total %.2f hours" % search_duration_hours,
            code=Result.CODE_DURATION,
            bad=False,
            value=search_duration_hours,
        ))

    def check_shards(self):
        shards_data = self._load_json("shards.json")
        shards_count = len(shards_data)

        if shards_count > 20000:
            self.results.append(Result(
                "Cluster has %s shards, that can cause some instability" % shards_count,
                code=Result.CODE_OVERSHARDING,
                bad=True,
                value=shards_count,
            ))
        else:
            self.results.append(Result(
                "Cluster has %s shards, that should not cause any issues" % shards_count,
                code=Result.CODE_OVERSHARDING,
                bad=True,
                value=shards_count,
            ))

        self.charts.append("Shards by doc count (millions)")
        self.charts.append(plotille.histogram([int(s["docs"]) / 1024 / 1024 for s in shards_data if s.get("docs")], height=10, x_min=0, x_max=100))

        self.charts.append("Shards by disk size (GB)")
        shard_sizes_gb = [int(s["store"]) / self.GB for s in shards_data if s.get("store")]
        self.charts.append(plotille.histogram(shard_sizes_gb, height=10, x_min=0, x_max=100))

        small_shards_count = len([s for s in shard_sizes_gb if s < 1])
        large_shards_count = len([s for s in shard_sizes_gb if s > 50])

        if small_shards_count > 0.1 * shards_count:
            self.results.append(Result(
                "Cluster has %s (%.2f%%) small (less than 1 GB) shards, shrinking or merging recommended" % (small_shards_count, small_shards_count / shards_count * 100),
                code=Result.CODE_MANY_SMALL_SHARDS,
                bad=True,
                value=small_shards_count,
            ))
        else:
            self.results.append(Result(
                "Cluster has %s (%.2f%%) small (less than 1 GB) shards" % (small_shards_count, small_shards_count / shards_count * 100),
                code=Result.CODE_MANY_SMALL_SHARDS,
                bad=False,
                value=small_shards_count,
            ))

        if large_shards_count > 0:
            self.results.append(Result(
                "Cluster has %s (%.2f%%) large (more than 50 GB) shards" % (large_shards_count, large_shards_count / shards_count * 100),
                code=Result.CODE_MANY_LARGE_SHARDS,
                bad=True,
                value=large_shards_count,
            ))
        else:
            self.results.append(Result(
                "Cluster has %s (%.2f%%) large (more than 50 GB) shards" % (large_shards_count, large_shards_count / shards_count * 100),
                code=Result.CODE_MANY_LARGE_SHARDS,
                bad=False,
                value=large_shards_count,
            ))

        cluster_state_size = os.path.getsize(os.path.join(self.root_path, "cluster_state.json"))
        cluster_state_size_mb = cluster_state_size / 1024 / 1024

        if cluster_state_size_mb > 50:
            self.results.append(Result(
                "Cluster state size is %.2f MB; this might cause various issues across the cluster" % cluster_state_size_mb,
                code=Result.CODE_CLUSTER_STATE_SIZE,
                bad=True,
                value=cluster_state_size_mb,
            ))
        else:
            self.results.append(Result(
                "Cluster state size is %.2f MB" % cluster_state_size_mb,
                code=Result.CODE_CLUSTER_STATE_SIZE,
                bad=False,
                value=cluster_state_size_mb,
            ))

        shards_by_node = {}

        for s in shards_data:
            if s["node"] not in shards_by_node:
                shards_by_node[s["node"]] = 0
            
            shards_by_node[s["node"]] += 1

        self.charts.append("Nodes by shard count")
        self.charts.append(plotille.histogram(shards_by_node.values(), height=10, x_min=0))

    def check_settings(self):
        settings_data = self._load_json("settings.json").values()
        indices_count = len(settings_data)
        refresh_1s_indices_count = len([i for i in settings_data if i["settings"]["index"].get("refresh_interval", "1s")])

        if refresh_1s_indices_count / indices_count > 0.1:
            self.results.append(Result(
                "refresh_interval is default 1s for %s indices (%.2f%%), consider raising to 30s or 60s to speed up ingestion" % (refresh_1s_indices_count, refresh_1s_indices_count / indices_count * 100),
                code=Result.CODE_REFRESH_INTERVAL,
                bad=False,
                value=refresh_1s_indices_count,
            ))
        else:
            self.results.append(Result(
                "refresh_interval is default 1s for %s indices (%.2f%%), that's ok" % (refresh_1s_indices_count, refresh_1s_indices_count / indices_count * 100),
                code=Result.CODE_REFRESH_INTERVAL,
                bad=False,
                value=refresh_1s_indices_count,
            ))

    def check_fielddata(self):
        fielddata_stats = self._load_json("fielddata_stats.json")

        field_sizes = {}

        for n in fielddata_stats["nodes"].values():
            for f, fdata in n["indices"]["fielddata"].get("fields", {}).items():
                if f not in field_sizes:
                    field_sizes[f] = 0
                field_sizes[f] += fdata["memory_size_in_bytes"]

        field_sizes_list = list(field_sizes.items())
        field_sizes_list.sort(key=lambda fs: fs[1])
        field_sizes_list.reverse()

        self.charts.append("Fields cardinality")
        table = rich.table.Table(title="Top 10 largest fields")
        table.add_column("Field", justify="right", style="cyan", no_wrap=True)
        table.add_column("Size (GB)", style="magenta")
        for f, fs in field_sizes_list[:10]:
            table.add_row(f, "%.2f" % (fs / self.GB))

        self.charts.append(table)

        self.charts.append("Zero-data fields (consider removal from mappings)")
        self.charts.append(" * " + f)

    def check_id_field_type(self):
        mappings_data = self._load_json("mapping.json")
        problematic_indices = []

        for index, mapping in mappings_data.items():
            if "properties" in mapping["mappings"]:
                fields_data = mapping["mappings"]["properties"]
            else:
                fields_data = mapping["mappings"]
            for field, field_data in fields_data.items():
                if field == "_id" and field_data.get("type") != "keyword":
                    problematic_indices.append(index)

        if problematic_indices:
            for index in problematic_indices:
                self.results.append(Result(
                    "Field _id in index %s is not of type keyword" % index,
                    code="NON_KEYWORD_ID_FIELD",
                    bad=True,
                    value=index,
                ))
        else:
            self.results.append(Result(
                "All _id fields are of type keyword",
                code="NON_KEYWORD_ID_FIELD",
                bad=False,
            ))

        dynamic_mapping_enabled = []
        for index, mapping in mappings_data.items():
            if mapping["mappings"].get("dynamic", "true") == "true":
                dynamic_mapping_enabled.append(index)

        if dynamic_mapping_enabled:
            if len(dynamic_mapping_enabled) < 10:
                self.results.append(Result(
                    "Dynamic mapping is enabled for indices: %s" % ", ".join(dynamic_mapping_enabled),
                    code="DYNAMIC_MAPPING_ENABLED",
                    bad=True,
                    value=dynamic_mapping_enabled,
                ))
            else:
                self.results.append(Result(
                    "Dynamic mapping is enabled for %s indices" % len(dynamic_mapping_enabled),
                    code="DYNAMIC_MAPPING_ENABLED",
                    bad=True,
                    value=dynamic_mapping_enabled,
                ))
        else:
            self.results.append(Result(
                "Dynamic mapping is disabled for all indices",
                code="DYNAMIC_MAPPING_ENABLED",
                bad=False,
            ))

    def check_custom_fields(self):
        mappings_data = self._load_json("mapping.json")
        custom_fields_indices = []
        problematic_fields = []

        for index, mapping in mappings_data.items():
            if 'properties' in mapping["mappings"]:
                fields_data = mapping["mappings"]["properties"]
            else:
                fields_data = mapping["mappings"]
            for field, field_data in fields_data.items():
                if field.startswith("custom_"):
                    custom_fields_indices.append(index)
                    break

                if field_data.get("type") == "text" and field_data.get("fielddata", False):
                    problematic_fields.append((index, field))

        if custom_fields_indices:
            for index in custom_fields_indices:
                self.results.append(Result(
                    "Custom fields found in index %s" % index,
                    code="CUSTOM_FIELDS",
                    bad=False,
                    value=index,
                ))
        else:
            self.results.append(Result(
                "No custom fields found in any index",
                code="CUSTOM_FIELDS",
                bad=False,
            ))

        if problematic_fields:
            for index, field in problematic_fields:
                self.results.append(Result(
                    "Field %s in index %s has fielddata enabled, which can cause performance issues" % (field, index),
                    code="PROBLEMATIC_FIELD_DATA_TYPE",
                    bad=True,
                    value=(index, field),
                ))
        else:
            self.results.append(Result(
                "No problematic field data types found",
                code="PROBLEMATIC_FIELD_DATA_TYPE",
                bad=False,
            ))

    def check_field_count(self):
        mappings_data = self._load_json("mapping.json")
        high_field_count_indices = []

        for index, mapping in mappings_data.items():
            if 'properties' in mapping["mappings"]:
                field_count = len(mapping["mappings"]["properties"])
            else:
                field_count = len(mapping["mappings"])
            if field_count > 200:  # arbitrary threshold for high field count
                high_field_count_indices.append((index, field_count))

        if high_field_count_indices:
            for index, count in high_field_count_indices:
                self.results.append(Result(
                    "High field count in index %s: %d fields" % (index, count),
                    code="HIGH_FIELD_COUNT",
                    bad=True,
                    value=count,
                ))
        else:
            self.results.append(Result(
                "Field count is within acceptable limits for all indices",
                code="HIGH_FIELD_COUNT",
                bad=False,
            ))

    def check_nested_fields(self):
        mappings_data = self._load_json("mapping.json")
        high_nested_field_indices = []

        for index, mapping in mappings_data.items():
            if 'properties' in mapping["mappings"]:
                fields_data = mapping["mappings"]["properties"]
            else:
                fields_data = mapping["mappings"]

            nested_field_count = sum(1 for field in fields_data.values() if field.get("type") == "nested")
            if nested_field_count > 5:  # arbitrary threshold for high nested field count
                high_nested_field_indices.append((index, nested_field_count))

        if high_nested_field_indices:
            for index, count in high_nested_field_indices:
                self.results.append(Result(
                    "High nested field count in index %s: %d nested fields" % (index, count),
                    code="HIGH_NESTED_FIELD_COUNT",
                    bad=True,
                    value=count,
                ))
        else:
            self.results.append(Result(
                "Nested field count is within acceptable limits for all indices",
                code="HIGH_NESTED_FIELD_COUNT",
                bad=False,
            ))

    def check_hot_threads(self):
        with open(os.path.join(self.root_path, "nodes_hot_threads.txt")) as f:
            hot_threads_raw = f.readlines()
        
        hot_threads = []
        bad_re = re.compile(r"^\s*9\d.\d\%")
        bad_lines = []
        bad_block = False
        for l in hot_threads_raw:
            if not l.strip() and any(bad_lines):
                # end of block
                hot_threads.append(bad_lines[:10])
                continue

            if bad_re.match(l):
                bad_lines = [l.strip()]
                bad_block = True
                continue

            if bad_block:
                bad_lines.append(l.strip())

        if len(hot_threads) > 5:
            with open("hot_threads.txt", "w") as f:
                for t in hot_threads:
                    f.write("\n".join(t))
                    f.write("\n\n")
            self.results.append(Result(
                "%s hot threads detected; details written to hot_threads.txt" % len(hot_threads),
                code=Result.CODE_HOT_THREADS,
                bad=True,
                value=hot_threads,
            ))
        else:
            self.results.append(Result(
                "%s hot threads detected" % len(hot_threads),
                code=Result.CODE_HOT_THREADS,
                bad=False,
                value=hot_threads,
            ))

    def check_jvm_heap_usage(self):
        nodes_stats = self._load_json("nodes_stats.json")["nodes"].values()
        high_heap_nodes = []

        for n in nodes_stats:
            heap_usage = n["jvm"]["mem"]["heap_used_percent"]
            if heap_usage > 75:
                high_heap_nodes.append((n["name"], heap_usage))

        if high_heap_nodes:
            for node, heap in high_heap_nodes:
                self.results.append(Result(
                    "High JVM heap usage on node %s: %d%%" % (node, heap),
                    code="HIGH_JVM_HEAP_USAGE",
                    bad=True,
                    value=heap,
                ))
        else:
            self.results.append(Result(
                "JVM heap usage is within acceptable limits on all nodes",
                code="HIGH_JVM_HEAP_USAGE",
                bad=False,
            ))

    def check_pending_tasks(self):
        if not os.path.exists(os.path.join(self.root_path, "pending_tasks.json")):
            return

        pending_tasks = self._load_json("pending_tasks.json")["tasks"]
        pending_task_count = len(pending_tasks)

        if pending_task_count > 0:
            self.results.append(Result(
                "There are %d pending tasks in the cluster" % pending_task_count,
                code="PENDING_TASKS",
                bad=True,
                value=pending_task_count,
            ))
        else:
            self.results.append(Result(
                "There are no pending tasks in the cluster",
                code="PENDING_TASKS",
                bad=False,
            ))

    def check_cpu_usage(self):
        nodes_stats = self._load_json("nodes_stats.json")["nodes"].values()
        high_cpu_nodes = []

        for n in nodes_stats:
            cpu_usage = n["os"]["cpu"]["percent"]
            if cpu_usage > 80:
                high_cpu_nodes.append((n["name"], cpu_usage))

        if high_cpu_nodes:
            for node, cpu in high_cpu_nodes:
                self.results.append(Result(
                    "High CPU usage on node %s: %d%%" % (node, cpu),
                    code="HIGH_CPU_USAGE",
                    bad=True,
                    value=cpu,
                ))
        else:
            self.results.append(Result(
                "CPU usage is within acceptable limits on all nodes",
                code="HIGH_CPU_USAGE",
                bad=False,
            ))

    def check_disk_usage(self):
        nodes_stats = self._load_json("nodes_stats.json")["nodes"].values()
        low_disk_nodes = []

        for n in nodes_stats:
            disk_free = n["fs"]["total"]["available_in_bytes"] / self.GB
            if disk_free < 10:  # less than 10 GB free
                low_disk_nodes.append((n["name"], disk_free))

        if low_disk_nodes:
            for node, disk in low_disk_nodes:
                self.results.append(Result(
                    "Low disk space on node %s: %.2f GB free" % (node, disk),
                    code="LOW_DISK_SPACE",
                    bad=True,
                    value=disk,
                ))
        else:
            self.results.append(Result(
                "Disk space is within acceptable limits on all nodes",
                code="LOW_DISK_SPACE",
                bad=False,
            ))

    def check_memory_usage(self):
        nodes_stats = self._load_json("nodes_stats.json")["nodes"].values()
        high_memory_nodes = []

        for n in nodes_stats:
            memory_usage = n["os"]["mem"]["used_percent"]
            if memory_usage > 80:
                high_memory_nodes.append((n["name"], memory_usage))

        if high_memory_nodes:
            for node, memory in high_memory_nodes:
                self.results.append(Result(
                    "High memory usage on node %s: %d%%" % (node, memory),
                    code="HIGH_MEMORY_USAGE",
                    bad=True,
                    value=memory,
                ))
        else:
            self.results.append(Result(
                "Memory usage is within acceptable limits on all nodes",
                code="HIGH_MEMORY_USAGE",
                bad=False,
            ))

        high_disk_io_nodes = []

        for n in nodes_stats:
            disk_io = n["fs"]["io_stats"]["total"]["operations"]
            if disk_io > 1000000:  # arbitrary threshold for high disk I/O
                high_disk_io_nodes.append((n["name"], disk_io))

        if high_disk_io_nodes:
            for node, io in high_disk_io_nodes:
                self.results.append(Result(
                    "High disk I/O on node %s: %d operations" % (node, io),
                    code="HIGH_DISK_IO",
                    bad=True,
                    value=io,
                ))
        else:
            self.results.append(Result(
                "Disk I/O is within acceptable limits on all nodes",
                code="HIGH_DISK_IO",
                bad=False,
            ))

    def check_unassigned_shards(self):
        cluster_health = self._load_json("cluster_health.json")
        unassigned_shards = cluster_health["unassigned_shards"]

        if unassigned_shards > 0:
            self.results.append(Result(
                "There are %d unassigned shards in the cluster" % unassigned_shards,
                code="UNASSIGNED_SHARDS",
                bad=True,
                value=unassigned_shards,
            ))
        else:
            self.results.append(Result(
                "There are no unassigned shards in the cluster",
                code="UNASSIGNED_SHARDS",
                bad=False,
            ))

    def check_disk_watermark(self):
        nodes_stats = self._load_json("nodes_stats.json")["nodes"].values()
        high_disk_nodes = []

        for n in nodes_stats:
            disk_free = n["fs"]["total"]["free_in_bytes"] / n["fs"]["total"]["total_in_bytes"] * 100
            if disk_free < 15:
                high_disk_nodes.append((n["name"], disk_free))

        if high_disk_nodes:
            for node, disk in high_disk_nodes:
                self.results.append(Result(
                    "Disk usage on node %s has exceeded the high watermark: %d%% free" % (node, disk),
                    code="DISK_WATERMARK_EXCEEDED",
                    bad=True,
                    value=disk,
                ))
        else:
            self.results.append(Result(
                "Disk usage is within acceptable limits on all nodes",
                code="DISK_WATERMARK_EXCEEDED",
                bad=False,
            ))
        nodes_stats = self._load_json("nodes_stats.json")["nodes"].values()
        high_cpu_nodes = []

        for n in nodes_stats:
            cpu_usage = n["os"]["cpu"]["percent"]
            if cpu_usage > 80:
                high_cpu_nodes.append((n["name"], cpu_usage))

        if high_cpu_nodes:
            for node, cpu in high_cpu_nodes:
                self.results.append(Result(
                    "High CPU usage on node %s: %d%%" % (node, cpu),
                    code="HIGH_CPU_USAGE",
                    bad=True,
                    value=cpu,
                ))
        else:
            self.results.append(Result(
                "CPU usage is within acceptable limits on all nodes",
                code="HIGH_CPU_USAGE",
                bad=False,
            ))

    def check_index_settings(self):
        settings_data = self._load_json("settings.json")

        for index, settings in settings_data.items():
            index_settings = settings["settings"]["index"]

            # Check number of replicas
            number_of_replicas = int(index_settings.get("number_of_replicas", 1))
            if number_of_replicas < 1:
                self.results.append(Result(
                    "Index %s has less than 1 replica" % index,
                    code="LOW_NUMBER_OF_REPLICAS",
                    bad=True,
                    value=number_of_replicas,
                ))

    def check(self):
        self.check_cluster_health()
        self.check_memory_usage()
        self.check_unassigned_shards()
        self.check_disk_watermark()
        self.check_jvm_heap_usage()
        self.check_pending_tasks()
        self.check_cpu_usage()
        self.check_disk_usage()
        self.check_nodes()
        self.check_settings()
        self.check_indices()
        self.check_shards()
        self.check_fielddata()
        self.check_field_count()
        self.check_nested_fields()
        self.check_hot_threads()
        self.check_id_field_type()
        self.check_custom_fields()
        self.check_index_settings()
        self.check_cpu_usage()
        self.check_disk_usage()
        return self

    def render(self):
        good = list(filter(lambda r: r.is_good(), self.results))
        bad = list(filter(lambda r: r.is_bad(), self.results))

        if any(bad):
            self.console.print("BAD:", style="bold red")
            for msg in bad:
                self.console.print(" * ", msg.get_message(), style="bold red")

        if any(self.charts):
            self.console.print("CHARTS:", style="bold yellow")
            for msg in self.charts:
                self.console.print(msg, style="yellow")

        if any(good):
            self.console.print("GOOD:", style="bold green")
            for msg in good:
                self.console.print(" * ", msg.get_message(), style="green")    
        
        return self

if __name__ == "__main__":
    Analyzer(sys.argv[1]).check().render()
