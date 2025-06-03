import os.path
import time
from typing import Any
import joblib
import torch

import torch.nn
import torch.nn.functional as F
from torch_geometric.data import Data

from tristar_adapter.graph_construct.graph import Graph


class OnlineService:
    _model = None
    _model_prefix = "models/"
    _model_postfix = ".pt"
    _model_name = "rule"

    def __init__(self, workload: str = None):
        if self._model_name == "bayes":
            model_path = self._model_prefix + "ycsb.pkl"
            self._model = joblib.load(model_path)
            return
        elif self._model_name == "rule":
            return
        if workload is not None:
            model_path = self._model_prefix + workload + self._model_postfix
            if torch.cuda.is_available():
                self.device = torch.device('cuda')
            else:
                self.device = torch.device('cpu')
            if os.path.exists(model_path) and os.path.isfile(model_path):
                self._model = torch.load(model_path)
                print(torch.cuda.is_available())  # 应为 True
                print(torch.version.cuda)         # 应与 nvcc 版本一致

    def service(self, service_name: str, *args: Any, **kwargs: Any) -> Any:
        if service_name.lower() == "predict":
            print(args[0])
            if self._model_name == "bayes":
                return self.predict_bayes(args[0])
            elif self._model_name == "rule":
                return self.predict_rule(args[0])
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

    def predict_bayes(self, filepath: str) -> int:
        start_load_time = time.time_ns()
        g = Graph(filepath)
        feature_g = [[g.read_cnts, g.write_cnts, g.rw_cnt, g.ww_cnt]]
        assert self._model is not None
        y_pred = self._model.predict(feature_g)
        return y_pred[0]

    def predict_rule(self, filepath: str) -> int:
        start_load_time = time.time_ns()
        g = Graph(filepath)
        res = self.my_rule(g.write_cnts, g.read_cnts)
        return res

    def my_rule(self, w: int, r: int) -> int:
        wr = w / (w + r)
        if wr < 0.2:
            return 1
        elif wr <= 0.4:
            return 0
        else:
            return 2
