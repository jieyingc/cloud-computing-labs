import os
import re
from collections import defaultdict
from typing import Any, Dict, List

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

WORD_RE = re.compile(r"[A-Za-z']+")

def _tokenize(line: str) -> List[str]:
    return [w.lower() for w in WORD_RE.findall(line or "")]

@app.route(route="start-mapreduce", methods=["POST"])
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    try:
        payload = req.get_json()
    except ValueError:
        payload = {}
    instance_id = await client.start_new("MasterOrchestrator", None, payload)
    return client.create_check_status_response(req, instance_id)

@app.orchestration_trigger(context_name="context")
def MasterOrchestrator(context: df.DurableOrchestrationContext):
    payload = context.get_input() or {}

    if "lines" in payload and isinstance(payload["lines"], list):
        lines = payload["lines"]
        kvs = [[i, lines[i]] for i in range(len(lines))]
    else:
        container = payload.get("container") or os.getenv("MR_CONTAINER", "mrinputs")
        blobs = payload.get("blobs")
        kvs = yield context.call_activity("GetInputDataFn", {"container": container, "blobs": blobs})

    map_tasks = [context.call_activity("MapperFn", kv) for kv in kvs]
    map_results = yield context.task_all(map_tasks)

    shuffled = yield context.call_activity("ShufflerFn", map_results)

    reduce_tasks = [context.call_activity("ReducerFn", item) for item in shuffled]
    reduced = yield context.task_all(reduce_tasks)

    out = {k: v for (k, v) in reduced}
    sorted_items = sorted(out.items(), key=lambda x: (-x[1], x[0]))
    return {
        "num_input_lines": len(kvs),
        "unique_words": len(out),
        "top_30": sorted_items[:30],
        "all_counts": dict(sorted_items),
    }

@app.activity_trigger(input_name="args")
def GetInputDataFn(args: Dict[str, Any]):
    conn = os.getenv("MR_STORAGE_CONN") or os.getenv("AzureWebJobsStorage")
    if not conn:
        raise RuntimeError("Missing MR_STORAGE_CONN or AzureWebJobsStorage in env.")

    container = (args or {}).get("container") or os.getenv("MR_CONTAINER", "mrinputs")
    blobs = (args or {}).get("blobs")  # optional list

    svc = BlobServiceClient.from_connection_string(conn)
    container_client = svc.get_container_client(container)

    if not blobs:
        blobs = [b.name for b in container_client.list_blobs()]

    kvs: List[List[Any]] = []
    offset = 0
    for name in blobs:
        data = container_client.get_blob_client(name).download_blob().readall()
        text = data.decode("utf-8", errors="replace")
        for line in text.splitlines():
            kvs.append([offset, line])
            offset += 1
    return kvs

@app.activity_trigger(input_name="kv")
def MapperFn(kv: List[Any]):
    _, line = kv
    return [[w, 1] for w in _tokenize(line)]

@app.activity_trigger(input_name="map_outputs")
def ShufflerFn(map_outputs: List[List[List[Any]]]):
    grouped: Dict[str, List[int]] = defaultdict(list)
    for one_mapper in (map_outputs or []):
        for (word, val) in (one_mapper or []):
            grouped[word].append(int(val))
    return [[word, vals] for word, vals in grouped.items()]

@app.activity_trigger(input_name="item")
def ReducerFn(item: List[Any]):
    word, vals = item[0], item[1]
    return [word, sum(int(v) for v in (vals or []))]
