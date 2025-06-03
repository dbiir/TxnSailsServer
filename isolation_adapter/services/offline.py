import os
import time
from typing import Any

import numpy as np
import math
from sklearn.metrics import classification_report, accuracy_score
from sklearn.naive_bayes import GaussianNB
import torch.nn
import torch.nn.functional as F
from sklearn.model_selection import KFold, train_test_split
from torch import optim
from torch.utils.data import Subset
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader
import random
import joblib

from isolation_adapter.graph_construct.graph import Graph
from isolation_adapter.graph_training.train import GraphClassificationModel
from torch.cuda.amp import GradScaler, autocast


class OfflineService:
    # filepath prefix and postfix
    __model_prefix = "models/"
    __model_postfix = ".pt"
    # params
    model: torch.nn.Module
    optimizer = None
    epoch_size = 1000
    workload = None

    # static scenarios' graph
    __gs: list[list[Graph]] = []
    __g_labels: list[list[float]] = []
    __graph_batch: list[Data] = []

    def __init__(self, workload: str = None):
        # self.model = None
        self.workload = workload
        if workload is not None:
            model_path = self.__model_prefix + workload + self.__model_postfix
            if os.path.exists(model_path) and os.path.isfile(model_path):
                self.model = torch.load(model_path)
                self.optimizer = optim.Adam(self.model.parameters(), lr=0.01)
            else:
                self.model = None

    def service(self, service_name: str, *args: Any, **kwargs: Any) -> Any:
        if len(args) == 1:
            self.traverse_folders(args[0])
        elif len(args) > 1:
            for ts in args[2]:
                fpath = args[1] + "/" + args[0] + "/" + ts
                self.traverse_folders(fpath)
                print(fpath)
        if service_name.lower() == "train":
            return self.train()
        elif service_name.lower() == "train_rule":
            return self.train_rule()
        elif service_name.lower() == "train_bayse":
            return self.train_bayse()

    def __read_label_file(self, file_path: str) -> list[float]:
        labels = []
        with open(file_path, 'r') as file:
            for line in file:
                l = np.array([float(label) for label in line.strip().split(',')])
                result = np.where(l == 1.0, 1.0, 0.0)
                labels.extend(result)
        return labels

    def traverse_folders(self, folder_path):
        for entry in os.scandir(folder_path):
            if not entry.is_dir():
                continue
            flag = False
            for sub_entry in os.scandir(entry.path):
                if not sub_entry.is_file():
                    continue
                if 'label' in sub_entry.name:
                    flag = True
            if not flag:
                continue
            self.__gs.append([])
            for sub_entry in os.scandir(entry.path):
                if not sub_entry.is_file():
                    continue
                if 'label' in sub_entry.name:
                    self.__g_labels.append(self.__read_label_file(sub_entry.path))
                else:
                    self.__gs[-1].append(Graph(sub_entry.path))

            assert len(self.__gs) == len(self.__g_labels)

    def train(self):
        if self.model is None:
            self.model = GraphClassificationModel(in_channels=1, edge_in_channels=2, hidden_channels=64, out_channels=3)
            if torch.cuda.is_available():
                self.device = torch.device('cuda')
            else:
                self.device = torch.device('cpu')
            self.model.to(self.device)
            self.optimizer = optim.Adam(self.model.parameters(), lr=0.005)
        print("len:", len(self.__gs), len(self.__g_labels))
        for i in range(len(self.__gs)):
            idx=0
            for g in self.__gs[i]:
                x = torch.tensor(g.nodes, dtype=torch.float).to(self.device)
                edge_index = torch.tensor(g.edges, dtype=torch.long).to(self.device)
                edge_attr = torch.tensor(g.edge_feature, dtype=torch.float).to(self.device)
                y = torch.tensor(self.__g_labels[i], dtype=torch.float16).to(self.device)
                x = F.normalize(x, p=2, dim=1)
                if edge_attr.dim() == 1:
                    edge_attr = F.normalize(edge_attr, p=2, dim=0)
                else:
                    edge_attr = F.normalize(edge_attr, p=2, dim=1)
                self.__graph_batch.append(
                    Data(x=x, edge_index=edge_index, edge_attr=edge_attr, y=y)
                )
                idx += 1

        train_data, test_data = train_test_split(self.__graph_batch, test_size=0.01, random_state=37)
        train_loader = DataLoader(train_data, batch_size=16, shuffle=True)
        test_loader = DataLoader(test_data, batch_size=16, shuffle=False)
        self.scaler = GradScaler()

        for epoch in range(1, self.epoch_size + 1):
            self.train_epoch(train_loader)
            train_acc = self.test_epoch(test_loader)
            print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}')
        
        torch.save(self.model, self.__model_prefix + self.workload + self.__model_postfix)

    def train_bayse(self):
        X, y = [], []
        print("len:", len(self.__gs), len(self.__g_labels))
        for i in range(len(self.__gs)):
            # if i > 200:
            #     continue
            idx=0
            for g in self.__gs[i]:
                feature_g = [g.read_cnts, g.write_cnts, g.rw_cnt, g.ww_cnt]
                label_g = self.__g_labels[i]
                X.append(feature_g)
                y.append(label_g)
                idx += 1
        y_labels = np.argmax(y, axis=1)
        print(y_labels)
        X_train, X_test, y_train, y_test = train_test_split(X, y_labels, test_size=0.2, random_state=42)
        gnb = GaussianNB()

        # train
        # gnb.fit(X, y_labels)
        gnb.fit(X_train, y_train)
        y_pred = gnb.predict(X_test)

        # evaluation
        model_path = self.__model_prefix + "ycsb.pkl"
        joblib.dump(gnb, model_path)

    '''latex
        \text{Snapshot Isolation}, & \text{if } wr < 0.2 \\
        \text{Serializable}, & \text{if } 0.2 \leq wr \leq 0.4 \\
        \text{Read Committed}, & \text{if } wr > 0.4
    '''
    def my_rule(self, w: int, r: int, l: list[float]) -> int:
        wr = w / (w + r)
        print(wr, l)
        if wr < 0.2:
            if abs(l[1] - 1) < 1e-3:
                return 1
            else:
                return 0
        elif wr <= 0.4:
            if abs(l[0] - 1) < 1e-3:
                return 1
            else:
                return 0
        else:
            if abs(l[2] - 1) < 1e-3:
                return 1
            else:
                return 0

    def train_rule(self):
        print("len:", len(self.__gs), len(self.__g_labels))
        cnt = 0
        correct_cnt = 0
        for i in range(len(self.__gs)):
            for g in self.__gs[i]:
                cnt += 1
                correct_cnt += self.my_rule(g.write_cnts, g.read_cnts, self.__g_labels[i])
        print("correct_cnt/cnt: {}/{} = {}", correct_cnt, cnt, correct_cnt / cnt)

    def train2(self):
        if self.model is None:
            self.model = GraphClassificationModel(in_channels=1, edge_in_channels=2, hidden_channels=256, out_channels=3)
            if torch.cuda.is_available():
                self.device = torch.device('cuda')
            else:
                self.device = torch.device('cpu')
            self.model.to(self.device)
            self.optimizer = optim.Adam(self.model.parameters(), lr=0.05)

        kfold = KFold(n_splits=10, shuffle=True)
        fold_results = []

        for i in range(len(self.__gs)):
            for g in self.__gs[i]:
                self.__graph_batch.append(
                    Data(x=torch.tensor(g.nodes, dtype=torch.float).to(self.device),
                         edge_index=torch.tensor(g.edges, dtype=torch.long).to(self.device),
                         edge_attr=torch.tensor(g.edge_feature, dtype=torch.float).to(self.device),
                         y=torch.tensor(self.__g_labels[i], dtype=torch.float16).to(self.device))
                )
        loader = DataLoader(self.__graph_batch, batch_size=64, shuffle=True)
        for fold, (train_idx, val_idx) in enumerate(kfold.split(loader.dataset)):
            print(f'FOLD {fold}')
            print('--------------------------------')

            # Sample elements randomly from a given list of indices, no replacement.
            train_subsampler = Subset(loader.dataset, train_idx)
            val_subsampler = Subset(loader.dataset, val_idx)

            # Define data loaders for training and validation
            train_loader = DataLoader(train_subsampler, batch_size=32, shuffle=True)
            val_loader = DataLoader(val_subsampler, batch_size=32, shuffle=False)

            # Initialize model, optimizer, etc.
            self.model.train()

            for data in train_loader:
                self.optimizer.zero_grad()
                out = self.model(data.x, data.edge_index, data.batch)
                out = out.flatten()
                loss = F.cross_entropy(out, data.y)
                loss.backward()
                self.optimizer.step()

            # Validation
            self.model.eval()
            val_loss = 0.0
            correct = 0
            total = 0

            with torch.no_grad():
                for data in val_loader:
                    out = self.model(data.x, data.edge_index, data.batch)
                    # out = out.flatten()
                    val_loss += F.cross_entropy(out.flatten(), data.y, reduction='sum').item()
                    # pred = out.argmax(dim=1)
                    # correct += pred.eq(data.y).sum().item()
                    total += (data.y.size(0) // 3)
                    max_values, _ = torch.max(out, dim=1)
                    result = torch.where(out == max_values.unsqueeze(1), 1.0, 0)
                    pred = result.flatten()
                    # print("pred: ", pred)
                    # print("data.y", data.y)
                    correct += ((pred == 1.0) & (data.y == 1.0)).sum().item()

            val_loss /= total
            accuracy = correct / total
            fold_results.append((val_loss, accuracy))
            print(f'Validation Loss: {val_loss:.4f}, Accuracy: {accuracy:.4f}')

        # Calculate average results
        avg_loss = sum([result[0] for result in fold_results]) / 10
        avg_accuracy = sum([result[1] for result in fold_results]) / 10
        print('--------------------------------')
        print(f'Average Validation Loss: {avg_loss:.4f}, Average Accuracy: {avg_accuracy:.4f}')

    def train_epoch(self, loader: DataLoader) -> None:
        self.model.train()
        for data in loader:
            self.optimizer.zero_grad()
            # print("data.batch" + int(data.batch))
            with autocast():
                out = self.model(data)
                out = out.flatten()
                loss = F.cross_entropy(out, data.y)
            # print(loss)
            # loss.backward()
            # self.optimizer.step()
            self.scaler.scale(loss).backward()
            self.scaler.step(self.optimizer)
            self.scaler.update()
        torch.cuda.empty_cache()

    def test_epoch(self, loader: DataLoader) -> float:
        self.model.eval()
        correct = 0
        for data in loader:
            out = self.model(data)
            max_values, _ = torch.max(out, dim=1)
            result = torch.where(out == max_values.unsqueeze(1), 1.0, 0)
            pred = result.flatten()
            # print("data.y", data.y)
            # print("pred", pred)
            correct += ((pred == 1.0) & (data.y == 1.0)).sum().item()
        return correct / len(loader.dataset)
