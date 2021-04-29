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

offset = 0
count = 10
levels = []
with urllib.request.urlopen(f"{bcd_url}?count={count}&offset={offset}") as url:
    data = json.loads(url.read().decode())
    operations = data["operations"]
    for operation in operations:
        levels.append(str(operation["level"]))

print(",".join(levels))
