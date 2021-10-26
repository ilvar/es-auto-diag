#/usr/bin/env python3

import json
import sys
import os

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

class Analyzer():
    root_path = None
    console = None

    results = []
    charts = []
    
    GB = 1024 * 1024 * 1024

    def __init__(self, root_path: str):
        self.root_path = root_path
        self.console = rich.console.Console()

    def _load_json(self, f: str) -> any: 
        return json.load(open(os.path.join(self.root_path, f)))

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

    def check_node_stats(self):
        nodes_stats = self._load_json("nodes_stats.json")["nodes"].values()
        thread_pool_rejections = {}

        for n in nodes_stats:
            for tp, tpd in n["thread_pool"].items():
                # TODO: check "largest" and "queue" as well
                if tp not in thread_pool_rejections:
                    thread_pool_rejections[tp] = 0
                
                thread_pool_rejections[tp] += tpd["rejected"]

        thread_pool_rejections_list = list(thread_pool_rejections.items())
        thread_pool_rejections_list.sort(key=lambda fs: fs[1])
        thread_pool_rejections_list.reverse()

        table = rich.table.Table(title="Thread pool rejections")
        table.add_column("Thread pool", justify="right", style="cyan", no_wrap=True)
        table.add_column("Rejections", style="magenta")
        for tp, tpr in thread_pool_rejections_list:
            if tpr > 0:
                table.add_row(tp, "%s" % tpr)

        self.charts.append(table)

        nodes_doc_counts = []
        nodes_disk_size_gb = []

        for n in nodes_stats:
            nodes_doc_counts.append(n["indices"]["docs"]["count"] / 1024 / 1024)
            nodes_disk_size_gb.append(n["indices"]["store"]["size_in_bytes"] / self.GB)

        self.charts.append("Nodes by doc count (millions)")
        self.charts.append(plotille.histogram(nodes_doc_counts, height=10, x_min=0))

        self.charts.append("Nodes by disk size (GB)")
        self.charts.append(plotille.histogram(nodes_disk_size_gb, height=10, x_min=0))

    def check(self):
        self.check_cluster_health()
        self.check_nodes()
        self.check_settings()
        self.check_shards()
        self.check_fielddata()
        self.check_node_stats()
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