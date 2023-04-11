import datetime
import igraph


def handler(event):
    size: int = event.get("size")

    graph_generating_begin: datetime.datetime = datetime.datetime.now()  # memory = C
    graph: igraph.Graph = igraph.Graph.Barabasi(size, 10)  # memory = A(size, 10)
    graph_generating_end: datetime.datetime = datetime.datetime.now()

    process_begin: datetime.datetime = datetime.datetime.now()
    result: list[int] = graph.spanning_tree(None, False)  # memory = B(None, False)
    process_end: datetime.datetime = datetime.datetime.now()

    graph_generating_time: float = (
        graph_generating_end - graph_generating_begin
    ) / datetime.timedelta(microseconds=1)
    process_time: float = (process_end - process_begin) / datetime.timedelta(
        microseconds=1
    )

    return {
        "result": result[0],
        "measurement": {
            "graph_generating_time": graph_generating_time,
            "compute_time": process_time,
        },
    }
