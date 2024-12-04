import json
import csv

def create_validator_csv(metrics_file, output_csv):
    with open(metrics_file, 'r') as file:
        data = json.load(file)

    validators = data["validators"]

    rows = []
    for validator in validators:
        total_blocks = validator["total_signed_blocks"] + validator["total_missed_blocks"]
        uptime = (
            validator["total_signed_blocks"] / total_blocks
            if total_blocks > 0
            else 0
        )
        rows.append({
            "Valoper": validator["valoper"],
            "Uptime": round(uptime * 100, 2),
            "Total Slashes": len(validator["slashes"]),
            "Tombstoned": validator["tombstoned"],
            "Blocks Active": total_blocks,
            "Signed Blocks": validator["total_signed_blocks"],
            "Missed Blocks": validator["total_missed_blocks"],
            "Proposed Blocks": validator["total_proposed_blocks"],
            "Delegators": validator["delegators_count"],
            "Stake": validator["stake"],
            "Self Stake": validator["self_stake"],
        })

    rows = sorted(rows, key=lambda x: x["Uptime"], reverse=True)

    with open(output_csv, 'w', newline='') as csvfile:
        fieldnames = [
            "Valoper",
            "Uptime",
            "Total Slashes",
            "Tombstoned",
            "Blocks Active",
            "Signed Blocks",
            "Missed Blocks",
            "Proposed Blocks",
            "Delegators",
            "Stake",
            "Self Stake",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV file '{output_csv}' created successfully.")
create_validator_csv("metrics.json", "validators.csv")