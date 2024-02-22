import csv
import json

name_iro = "walmart/iro-glass-2023-sep-27"
name_app1 = "walmart/samples-2023-oct-05/apps-1.ndjson"
name_app2 = "walmart/samples-2023-oct-05/apps-2.ndjson"
name_wcnp = "walmart/samples-2023-oct-05/wcnp_infra.ndjson"
name_mongo = "mongodb"
name_spark = "spark-event-logs"
name_rider = "uber/rider-product-core"

name_list = [
"walmart/iro-glass-2023-sep-27",
"walmart/samples-2023-oct-05/apps-1.ndjson",
"walmart/samples-2023-oct-05/apps-2.ndjson",
"walmart/samples-2023-oct-05/wcnp_infra.ndjson",
"mongodb",
"spark-event-logs",
"uber/rider-product-core",
]

def process(input_file):
    data_list = [[], [], [], [], [], [], []]
    with open(input_file, "r") as reader:
        for line in reader:
            data = json.loads(line.strip())
            name = data["path"]
            idx = None
            if "raw-iro" in name:
                idx = 0
            elif "apps-1" in name:
                idx = 1
            elif "apps-2" in name:
                idx = 2
            elif "wcnp" in name:
                idx = 3
            elif "mongod" in name:
                idx = 4
            elif "spark" in name:
                idx = 5
            elif "rider" in name:
                idx = 6
            else:
                print("WTF??")
                exit()
            row = []
            row.append(name_list[idx])
            row.append(data["num_lines"])
            # row.append(data["size_msgpack"])
            row.append(data["size_json"])
            row.append(data["time_clp_ir"])
            # row.append(data["time_map_to_msgpack"])
            # row.append(data["size_ir"])
            # row.append(data["time_json_to_msgpack"])
            # row.append(data["time_msgpack_to_map"])
            # row.append(data["time_map_to_ir"])
            # row.append(data["time_ir_deserialization"])
            data_list[idx].append(row)
    with open(f"{input_file[:-5]}.csv", mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "Dataset",
            "Num Events",
            # "msgpack size (B)",
            "json size (B)",
            # "map->msgpack time (s),"
            "clp ir time (s)"
            # "ir-v2 size (B)",
            # "JSON->msgpack time (s)",
            # "msgpack->map time (s)",
            # "map->ir-v2 time (s)",
            # "ir-v2->schema+values time (s)"
        ])
        for rows in data_list:
            for row in rows:
                writer.writerow(row)

if "__main__" == __name__:
    files = [
        # "eva_all_4byte_result.json",
        # "eva_all_8byte_result.json",
        # "eva_32_64_4byte_result.json",
        # "eva_32_64_8byte_result.json",
        # "eva_64_4byte_result.json",
        # "eva_64_8byte_result.json",
        "eva_result_clp_str.json"
    ]
    for file in files:
        process(file)