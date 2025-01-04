#!/usr/bin/python3
import os
from xml.etree import ElementTree
import xml.dom.minidom as minidom
from itertools import product

warmupTime = 20
execTime = 60  # ms

transactionType = [
    "NewOrder",
    "Payment",
    "OrderStatus",
    "Delivery",
    "StockLevel"
]

cc_map = {
    "SERIALIZABLE": "SER",
    "SI_ELT": "SI+E",
    "RC_ELT": "RC+E",
    "SI_FOR_UPDATE": "SI+P",
    "RC_FOR_UPDATE": "RC+P",
    "SI_TAILOR": "SI+TV",
    "RC_TAILOR": "RC+TV",
    "RC_TAILOR_LOCK": "RC+TL",
    "DYNAMIC": "DYNAMIC"
}


def generate_mysql_tpcc_config(cc_type: str, terminals, weight, zipf: float = 0.1, scalaF = 16, rate="", dir="../config", casename=""):
    # 创建根节点
    root = ElementTree.Element('parameters')
    # 添加子节点
    ElementTree.SubElement(root, 'type').text = "POSTGRES"
    ElementTree.SubElement(root, 'driver').text = "org.postgresql.Driver"
    ElementTree.SubElement(root, "url").text = ("jdbc:postgresql://localhost:5432/osprey?sslmode=disable&amp"
                                                ";ApplicationName=smallbank&amp;reWriteBatchedInserts=true")
    ElementTree.SubElement(root, "username").text = "postgres"
    ElementTree.SubElement(root, "password").text = "Ss123!@#"
    ElementTree.SubElement(root, "isolation").text = "TRANSACTION_SERIALIZABLE"
    ElementTree.SubElement(root, "batchsize").text = "128"
    ElementTree.SubElement(root, "concurrencyControlType").text = cc_type

    ElementTree.SubElement(root, "scalefactor").text = str(scalaF)
    ElementTree.SubElement(root, "terminals").text = str(terminals)

    if casename == "warehouse":
        ElementTree.SubElement(root, "warehouseSkew").text = "true"
    elif casename == "customer":
        ElementTree.SubElement(root, "customerSkew").text = "true"

    works = ElementTree.SubElement(root, "works")
    if int(len(rate)) == int(0):
        generate_work(works, weight, "unlimited")
    else:
        generate_work(works, weight, rate)
    transactions = ElementTree.SubElement(root, "transactiontypes")
    generate_transation(transactions)

    # 将根目录转化为树行结构
    ElementTree.ElementTree(root)
    rough_str = ElementTree.tostring(root, 'utf-8')
    # 格式化
    reparsed = minidom.parseString(rough_str)
    new_str = reparsed.toprettyxml(indent='\t')

    filename = "/terminal_" + str(terminals)
    filename += "_warehouse_{:03d}".format(scalaF)
    filename += "_zipf_{:03.2f}".format(zipf)
    if casename == "warehouse":
        filename += "_tn_warehouse_"
    elif casename == "customer":
        filename += "_tn_customer_"

    if len(rate):
        filename += "_rate_" + str(rate)

    filename += "_cc_" + cc_map[cc_type]

    f = open(dir + filename + ".xml", 'w', encoding='utf-8')
    f.write(new_str)
    f.close()


def generate_work(root: ElementTree, weights, rate):
    work = ElementTree.SubElement(root, "work")
    ElementTree.SubElement(work, "warmup").text = str(warmupTime)
    ElementTree.SubElement(work, "time").text = str(execTime)
    ElementTree.SubElement(work, "rate").text = rate
    ElementTree.SubElement(work, "weights").text = str(weights)[1:-1]


def generate_transation(root: ElementTree):
    for entry in transactionType:
        transaction = ElementTree.SubElement(root, "transactiontype")
        ElementTree.SubElement(transaction, "name").text = entry


def tpcc_skew_warehouse(terminal=128):
    dir_name = "../config/tpcc/skew_warehouse-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
    cc = ["SERIALIZABLE", "SI", "RC_ELT", "RC_FOR_UPDATE", "RC_TAILOR"]
    skews = [0.1, 0.3, 0.5, 0.7, 0.9, 1.1, 1.3]
    weight = [45, 43, 4, 4, 4]

    experiments = product(cc, [terminal], skews)
    for exp in experiments:
        generate_mysql_tpcc_config(exp[0], exp[1], weight, zipf=exp[2], dir=dir_name, casename="warehouse")


def tpcc_skew_customer(terminal=128):
    dir_name = "../config/tpcc/skew_custom-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
    cc = ["SERIALIZABLE", "SI", "RC_ELT", "RC_FOR_UPDATE", "RC_TAILOR"]
    skews = [0.1, 0.3, 0.5, 0.7, 0.9, 1.1, 1.3]
    weight = [45, 43, 4, 4, 4]

    experiments = product(cc, [terminal], skews)
    for exp in experiments:
        generate_mysql_tpcc_config(exp[0], exp[1], weight, exp[2], dir=dir_name, casename="customer")


def tpcc_warehouse(terminal=128):
    dir_name = "../config/tpcc/warehouse-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
    cc = ["SERIALIZABLE", "SI", "RC_ELT", "RC_FOR_UPDATE", "RC_TAILOR"]
    weight = [45, 43, 4, 4, 4]
    wn = [1, 2, 4, 8, 16]

    experiments = product(cc, [terminal], wn)
    for exp in experiments:
        generate_mysql_tpcc_config(exp[0], exp[1], weight, scalaF=exp[2], dir=dir_name)


def tpcc_scalability():
    dir_name = "../config/tpcc/scalability/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
    cc = ["SERIALIZABLE", "SI", "RC_ELT", "RC_FOR_UPDATE", "RC_TAILOR"]
    terminals = [4, 8, 16, 32, 64, 128, 256, 512]
    weight = [45, 43, 4, 4, 4]
    wn = [1, 2, 4, 8, 16]

    experiments = product(cc, terminals, wn)
    for exp in experiments:
        generate_mysql_tpcc_config(exp[0], exp[1], weight, scalaF=exp[2], dir=dir_name)


if __name__ == '__main__':
    if not os.path.exists("../config"):
        os.mkdir("../config")

    scaleFactor = 16

    tpcc_skew_warehouse()
    tpcc_skew_customer()
    tpcc_warehouse()
