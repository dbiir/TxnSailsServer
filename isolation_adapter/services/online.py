import os.path
import time
from typing import Any

import torch.nn
import torch.nn.functional as F
from torch_geometric.data import Data

from tristar_adapter.graph_construct.graph import Graph


class OnlineService:
    _model: torch.nn.Module = None
    _model_prefix = "models/"
    _model_postfix = ".pt"

    def __init__(self, workload: str = None):
        if workload is not None:
            model_path = self._model_prefix + workload + self._model_postfix
            if torch.cuda.is_available():
                self.device = torch.device('cuda')
            else:
                self.device = torch.device('cpu')
            if os.path.exists(model_path) and os.path.isfile(model_path):
                self._model = torch.load(model_path)

    def service(self, service_name: str, *args: Any, **kwargs: Any) -> Any:
        if service_name.lower() == "predict":
            print(args[0])
            return self.predict(args[0])
        if service_name.lower() == "ok":
            return "ok"

    def predict(self, filepath: str) -> int:
        start_load_time = time.time_ns()
        g = Graph(filepath)
        assert self._model is not None
        print("Data Loaded. time consuming: " + str((time.time_ns() - start_load_time) / 1000000.0) + "ms")
        x = torch.tensor(g.nodes, dtype=torch.float).to(self.device)
        edge_index = torch.tensor(g.edges, dtype=torch.long).to(self.device)
        edge_attr = torch.tensor(g.edge_feature, dtype=torch.float).to(self.device)
        x = F.normalize(x, p=2, dim=1)
        if edge_attr.dim() == 1:
            edge_attr = F.normalize(edge_attr, p=2, dim=0)
        else:
            edge_attr = F.normalize(edge_attr, p=2, dim=1)

        graph = Data(x=x, edge_index=edge_index, edge_attr=edge_attr)
        res = self._model(graph).argmax(dim=1).int().item()
        print("load + predict time consuming: " + str((time.time_ns() - start_load_time) / 1000000.0) + "ms")
        return res
