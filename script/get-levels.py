#!/usr/bin/env python3

import json
import sys
import urllib.request



if len(sys.argv) != 3:
    print(f"Usage {sys.argv[0]} network contract_address")
    exit(0)

network = sys.argv[1]
contract_id = sys.argv[2]

bcd_url = f"https://api.better-call.dev/v1/contract/{network}/{contract_id}/operations"

levels = []
last_id_query = ""
while True:
    u = f"{bcd_url}{last_id_query}"
    # print(u)
    with urllib.request.urlopen(u) as url:
        data = json.loads(url.read().decode())
        last_id = "0"
        if "last_id" in data:
            last_id = data["last_id"]
        if last_id == "0":
            break
        else:
            last_id_query=f"?last_id={last_id}"
        operations = data["operations"]
        for operation in operations:
            levels.append(str(operation["level"]))


# remove duplicates
levels = list(dict.fromkeys(levels))
levels.sort()
print(",".join(levels))
