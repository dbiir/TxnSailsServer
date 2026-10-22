#!/usr/bin/env python3
"""Exercise single, distributed or mock mode using the unified TOML (Python 3.11+)."""

import argparse
from pathlib import Path
import socket
import tomllib


def run(node, placement, single=False):
    with socket.create_connection((node.get("host", "127.0.0.1"), node["client_port"]),
                                  timeout=15) as connection:
        with connection.makefile("rw", encoding="utf-8", newline="\n") as stream:
            def call(command):
                stream.write(command + "\n")
                stream.flush()
                response = stream.readline().rstrip("\n")
                if response != "OK" and not response.startswith("OK#"):
                    raise RuntimeError(f"{command}: {response or 'connection closed'}")
                return response

            def value(response):
                data = response[3:]
                return data[6:6 + int(data[2:6], 16)]

            owner = node["id"]
            if single:
                call("register#demo_write#write#usertable#UPDATE usertable SET field1 = ? WHERE ycsb_key = ?")
                call("register#demo_read#read#usertable#SELECT field1 FROM usertable WHERE ycsb_key = ?")
                call("analysis")
                call("begin")
                call(f"execute#demo_write#0#single-owner{owner}#0")
                call("commit")
                call("begin")
                result = value(call("execute#demo_read#0#0"))
                call("commit")
                print(f"Single node {owner}: committed and read key 0 = {result}")
                return

            call("begin#write_pair")
            for key, target in enumerate(placement):
                call(f"execute#write_pair#{key % 2}#coordinator{owner}-owner{target}#{key}")
            call("commit")
            print(f"Coordinator {owner}: committed writes to logical owners {placement}")
            call("begin#read_pair")
            for key in range(len(placement)):
                result = value(call(f"execute#read_pair#{key % 2}#{key}"))
                print(f"  Key {key}: {result}")
            call("commit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/txnsails.toml"))
    parser.add_argument("--mode", choices=("single", "distributed", "mock"))
    parser.add_argument("--coordinator", default="all", help="Configured node ID, or all")
    args = parser.parse_args()
    with args.config.open("rb") as source:
        config = tomllib.load(source)
    mode = args.mode or config.get("mode", "single")
    selected = config[mode]
    if mode == "single":
        nodes = [{"id": selected.get("node_id", 0),
                  "client_port": selected.get("client_port", 9876)}]
        placement = [nodes[0]["id"]]
    else:
        nodes = selected["nodes"]
        placement = selected["tables"]["usertable"]
    if args.coordinator != "all":
        try:
            coordinator = int(args.coordinator)
        except ValueError:
            parser.error("--coordinator must be a node ID or all")
        nodes = [node for node in nodes if node["id"] == coordinator]
        if not nodes:
            parser.error(f"Unknown coordinator {coordinator} for mode {mode}")
    for node in nodes:
        run(node, placement, mode == "single")


if __name__ == "__main__":
    main()
