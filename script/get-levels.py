#!/usr/bin/env python3

import json
import sys
import urllib.request



if len(sys.argv) != 3:
    print(f"Usage {sys.argv[0]} network contract_address")
    exit(0)

network = sys.argv[1]
contract_id = sys.argv[2]
bcd_timeout = 30  # seconds


def get_latest_level(bcd_baseurl):
    u = f"{bcd_baseurl}/head"
    with urllib.request.urlopen(u, timeout=bcd_timeout) as url:
        for x in json.loads(url.read().decode()):
            if x["network"] == network:
                return x["level"]
    raise ValueError(f"failed to find latest level for network={network}")


bcd_baseurl = "https://api.better-call.dev/v1"

# adding current <head> to blocks, in order to know up to where we can safely
# assign empty set of results to unrelated blocks in the db (see the --init
# command of the indexer)
head = get_latest_level(bcd_baseurl)
levels = [str(head)]

last_id_query = ""
bcd_ops_url = f"{bcd_baseurl}/contract/{network}/{contract_id}/operations"
while True:
    u = f"{bcd_ops_url}{last_id_query}"
    # print(u)
    with urllib.request.urlopen(u, timeout=bcd_timeout) as url:
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
