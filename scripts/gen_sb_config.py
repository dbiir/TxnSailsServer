#!/usr/bin/python3
import os
from xml.etree import ElementTree
import xml.dom.minidom as minidom
from itertools import product
import random

scaleFactor = 4
warmupTime = 20
execTime = 60  # ms
maxRetry = 16

transactionType = [
    "Amalgamate",
    "Balance",
    "DepositChecking",
    "SendPayment",
    "TransactSavings",
    "WriteCheck"
]

cc_map = {
    "SERIALIZABLE": "SER",
    "SI_ELT": "SI+E",
    "RC_ELT": "RC+E",
    "SI_FOR_UPDATE": "SI+P",
    "RC_FOR_UPDATE": "RC+P",
    "SI_TAILOR": "SI+TV",
    "RC_TAILOR": "RC+TV",
    "DYNAMIC": "DYNAMIC"
}


def generate_pg_sb_config(cc_type: str, terminals, weight, hsn=-1, hsp=-1.0, zipf=-0.1, rate: str="",
                          dir="../config", case_name=""):
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

    if zipf > 0:
        ElementTree.SubElement(root, "zipf").text = str(zipf)
    if hsn > 0 and hsp > 0:
        ElementTree.SubElement(root, "hotspotNumber").text = str(hsn)
        ElementTree.SubElement(root, "hotspotPercentage").text = str(hsp)
    ElementTree.SubElement(root, "scalefactor").text = str(scaleFactor)
    ElementTree.SubElement(root, "terminals").text = str(terminals)

    works = ElementTree.SubElement(root, "works")
    if len(rate) == 0:
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
    if zipf > 0:
        filename += "_zipf_{:03.2f}".format(zipf)
    elif hsn > 0 and hsp > 0:
        filename += "_hsn_{:05d}_hsp_{:03.2f}".format(hsn, hsp)

    if len(rate):
        filename += "_rate_" + str(rate)
    if len(case_name) > 0:
        filename += "_" + case_name + "_" + '-'.join(["{:03.1f}".format(w) for w in weight])

    filename += "_cc_" + cc_map[cc_type]

    f = open(dir + filename + ".xml", 'w', encoding='utf-8')
    f.write(new_str)
    f.close()


def generate_work(root: ElementTree, weights, rate="unlimited"):
    work = ElementTree.SubElement(root, "work")
    ElementTree.SubElement(work, "warmup").text = str(warmupTime)
    ElementTree.SubElement(work, "time").text = str(execTime)
    ElementTree.SubElement(work, "rate").text = rate
    ElementTree.SubElement(work, "retries").text = str(maxRetry)
    ElementTree.SubElement(work, "weights").text = str(weights)[1:-1]


def generate_transation(root: ElementTree):
    for entry in transactionType:
        transaction = ElementTree.SubElement(root, "transactiontype")
        ElementTree.SubElement(transaction, "name").text = entry


def sb_scalability():
    dir_name = "../config/smallbank/scalability/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    terminals = [4, 8, 16, 32, 64, 128, 256, 512]
    # terminals = [128]
    # , "RC_TAILOR_LOCK", "DYNAMIC"
    cc = ["SERIALIZABLE", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    # cc = ["SERIALIZABLE", "SI_ELT", "RC_ELT", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    # weight = list(default_weight_by_dis_ration(dis_ratio))
    weight = [20, 20, 20, 0, 20, 20]

    experiments = product(cc, terminals)
    for exp in experiments:
        generate_pg_sb_config(exp[0], exp[1], weight, dir=dir_name)


def sb_hotspot(terminal=128):
    dir_name = "../config/smallbank/hotspot-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    # cc = ["RC_TAILOR", "SI_TAILOR"]
    cc = ["SERIALIZABLE", "SI_ELT", "RC_ELT", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]

    hsn_list = [10, 100, 1000]
    hsp_list = [0.1, 0.3, 0.5, 0.7, 0.9]
    weight = [20, 20, 20, 0, 20, 20]

    experiments = product(cc, hsn_list, hsp_list)
    for exp in experiments:
        generate_pg_sb_config(exp[0], terminals=terminal, weight=weight, hsn=exp[1], hsp=exp[2], dir=dir_name)


def sb_zip_fain(terminal=128):
    dir_name = "../config/smallbank/skew-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    # cc = ["RC_TAILOR"]
    cc = ["SERIALIZABLE", "SI_ELT", "RC_ELT", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    # skew_list = [0.7, 1.1]
    skew_list = [0.1, 0.3, 0.5, 0.7, 0.9, 1.1, 1.3]
    weight = [20, 20, 20, 0, 20, 20]

    experiments = product(cc, skew_list)
    for exp in experiments:
        generate_pg_sb_config(exp[0], terminals=terminal, weight=weight, zipf=exp[1], dir=dir_name)


def sb_bal_ratio(terminal=128):
    dir_name = "../config/smallbank/bal_ratio-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    cc = ["SERIALIZABLE", "SI_ELT", "RC_ELT", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    skew_list = [0.3, 0.7, 1.1]
    # weights = [[20, 20, 20, 0, 20, 20], [15, 40, 15, 0, 15, 15], [10, 60, 10, 0, 10, 10], [5, 80, 5, 0, 5, 5],
    #            [0, 90, 0, 0, 0, 0]]
    weights = [[22.5, 10, 22.5, 0, 22.5, 22.5], [17.5, 30, 17.5, 0, 17.5, 17.5], [12.5, 50, 12.5, 0, 12.5, 12.5], [7.5, 70, 7.5, 0, 7.5, 7.5],
               [2.5, 90, 2.5, 0, 2.5, 2.5]]

    experiments = product(cc, weights, skew_list)
    for exp in experiments:
        generate_pg_sb_config(exp[0], terminals=terminal, weight=exp[1], zipf=exp[2], dir=dir_name, case_name="Balance")


def sb_wc_ratio(terminal=128):
    dir_name = "../config/smallbank/wc_ratio-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    cc = ["SERIALIZABLE", "RC_FOR_UPDATE", "SI_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    # cc = ["SERIALIZABLE", "SI_ELT", "RC_ELT", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    skew_list = [0.3, 0.7, 1.1]
    # weights = [[20, 20, 20, 0, 20, 20], [15, 15, 15, 0, 15, 40], [10, 10, 10, 0, 10, 60], [5, 5, 5, 0, 5, 80],
    #            [0, 0, 0, 0, 0, 100]]
    weights = [[22.5, 22.5, 22.5, 0, 22.5, 10], [17.5, 17.5, 17.5, 0, 17.5, 30], [12.5, 12.5, 12.5, 0, 12.5, 50], [7.5, 7.5, 7.5, 0, 7.5, 70],
               [2.5, 2.5, 2.5, 0, 2.5, 90]]

    experiments = product(cc, weights, skew_list)
    for exp in experiments:
        generate_pg_sb_config(exp[0], terminals=terminal, weight=exp[1], zipf=exp[2], dir=dir_name, case_name="WriteCheck")


def sb_rate(terminal=128):
    dir_name = "../config/smallbank/rate-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    cc = ["SERIALIZABLE", "SI_ELT", "RC_ELT", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    hsn_list = [10, 100, 1000]
    hsp_list = [0.5]
    rates = [5000, 10000, 15000, "unlimited"]
    weight = [20, 20, 20, 0, 20, 20]

    experiments = product(cc, hsn_list, hsp_list, rates)
    for exp in experiments:
        generate_pg_sb_config(exp[0], terminals=terminal, weight=weight, hsn=exp[1], hsp=exp[2], rate=str(exp[3]), dir=dir_name)


def sb_random(terminal=128, cnt=80):
    dir_name = "../config/smallbank/random-" + str(terminal) + "/postgresql"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # cc = ["SERIALIZABLE", "SI_ELT", "RC_ELT", "SI_FOR_UPDATE", "RC_FOR_UPDATE", "RC_TAILOR", "SI_TAILOR"]
    cc = ["SERIALIZABLE", "RC_TAILOR", "SI_TAILOR"]
    for i in range(cnt):
        rf1, rf2 = random.uniform(0, 1), random.uniform(0, 1)
        skew = random.uniform(0.1, 1.3)
        hsn = random.randint(10, 5000)
        hsp = random.uniform(0.1, 0.9)
        rate = random.randint(5000, 20000)
        if rf2 < 1:
            rate = "unlimited"
        weight = rand_weight()
        if rf1 < 0.5:
            # skew
            for c in cc:
                generate_pg_sb_config(c, terminals=terminal, weight=weight, zipf=skew, rate=str(rate), dir=dir_name, case_name="R")
        else:
            # hotspot
            for c in cc:
                generate_pg_sb_config(c, terminals=terminal, weight=weight, hsn=hsn, hsp=hsp, rate=str(rate), dir=dir_name, case_name="R")


def rand_weight() -> list:
    r_weight = []
    total = 100
    for i in range(4):
        r_int = random.randint(0, total)
        r_weight.append(r_int)
        total -= r_int

    r_weight.append(total)
    random.shuffle(r_weight)
    r_weight.insert(3, 0)
    return r_weight


if __name__ == '__main__':
    if not os.path.exists("../config"):
        os.mkdir("../config")

    # warmupTime = 10
    # execTime = 30
    sb_scalability()
    warmupTime = 5
    execTime = 15
    # sb_hotspot(256)
    # sb_zip_fain(128)
    # sb_bal_ratio(256)
    # sb_wc_ratio(256)
    # sb_rate(128)
    sb_random(128, 200)
