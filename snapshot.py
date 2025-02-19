import requests
import json
from datetime import datetime
from collections import defaultdict

# REST API endpoint
REST_URL = "https://rest.unicorn.meme"

# Function to query with pagination
def query_with_pagination(endpoint, key=None):
    params = {"pagination.limit": "100"}
    if key:
        params["pagination.key"] = key
    try:
        response = requests.get(f"{REST_URL}{endpoint}", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        items = data.get("accounts", []) if "accounts" in data else data.get("metadatas", [])
        next_key = data.get("pagination", {}).get("next_key")
        return items, next_key
    except requests.RequestException as e:
        raise Exception(f"Failed to query {endpoint}: {str(e)}")

# Step 1: Collect all accounts
print("Fetching all accounts...")
accounts = []
next_key = None
while True:
    page_accounts, next_key = query_with_pagination("/cosmos/auth/v1beta1/accounts", next_key)
    for account in page_accounts:
        # For BaseAccounts, reset account_number and sequence for a new chain
        if account["@type"] == "/cosmos.auth.v1beta1.BaseAccount":
            account["account_number"] = "0"
            account["sequence"] = "0"
        accounts.append(account)
    if not next_key:
        break
print(f"Collected {len(accounts)} accounts.")

# Step 2: Collect balances for each account
print("Fetching balances...")
balances = []
for account in accounts:
    address = account["address"]
    try:
        response = requests.get(f"{REST_URL}/cosmos/bank/v1beta1/balances/{address}", timeout=10)
        response.raise_for_status()
        data = response.json()
        if "balances" in data and data["balances"]:
            balances.append({"address": address, "coins": data["balances"]})
    except requests.RequestException as e:
        print(f"Warning: Could not fetch balance for {address}: {str(e)}")
print(f"Collected balances for {len(balances)} accounts.")

# Step 3: Calculate total supply from balances
supply = defaultdict(int)
for balance in balances:
    for coin in balance["coins"]:
        supply[coin["denom"]] += int(coin["amount"])
supply_list = [{"denom": denom, "amount": str(amount)} for denom, amount in supply.items()]

# Step 4: Fetch additional bank module data
print("Fetching bank parameters and metadata...")
try:
    bank_params_response = requests.get(f"{REST_URL}/cosmos/bank/v1beta1/params").json()
    bank_params = bank_params_response.get("params", {"send_enabled": [], "default_send_enabled": True})
except Exception as e:
    print(f"Warning: Using default bank params due to error: {e}")
    bank_params = {"send_enabled": [], "default_send_enabled": True}

denom_metadata = []
next_key = None
while True:
    metas, next_key = query_with_pagination("/cosmos/bank/v1beta1/denoms_metadata", next_key)
    denom_metadata.extend(metas)
    if not next_key:
        break

# Step 5: Build the genesis file
default_genesis = {
    "genesis_time": datetime.utcnow().isoformat() + "Z",
    "chain_id": "unicorn-snapshot",
    "initial_height": "1",
    "consensus_params": {
        "block": {"max_bytes": "22020096", "max_gas": "-1"},
        "evidence": {"max_age_num_blocks": "100000", "max_age_duration": "172800000000000", "max_bytes": "1048576"},
        "validator": {"pub_key_types": ["ed25519"]},
        "version": {}
    },
    "app_hash": "",
    "app_state": {
        "auth": {
            "params": {
                "max_memo_characters": "256",
                "tx_sig_limit": "7",
                "tx_size_cost_per_byte": "10",
                "sig_verify_cost_ed25519": "590",
                "sig_verify_cost_secp256k1": "1000"
            },
            "accounts": []
        },
        "bank": {
            "params": bank_params,
            "balances": [],
            "supply": [],
            "denom_metadata": [],
            "send_enabled": bank_params.get("send_enabled", [])
        }
    }
}

# Update with fetched data
try:
    auth_params_response = requests.get(f"{REST_URL}/cosmos/auth/v1beta1/params").json()
    default_genesis["app_state"]["auth"]["params"] = auth_params_response.get("params", default_genesis["app_state"]["auth"]["params"])
except Exception as e:
    print(f"Warning: Using default auth params due to error: {e}")

default_genesis["app_state"]["auth"]["accounts"] = accounts
default_genesis["app_state"]["bank"]["balances"] = balances
default_genesis["app_state"]["bank"]["supply"] = supply_list
default_genesis["app_state"]["bank"]["denom_metadata"] = denom_metadata

# Step 6: Save to genesis.json
print("Saving to genesis.json...")
with open("genesis.json", "w") as f:
    json.dump(default_genesis, f, indent=2)
print("Snapshot complete! Check genesis.json.")
