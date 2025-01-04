import json

input_file_path = "server-m\metrics.json"
output_file_path = "metrics-end-day.json"
with open(input_file_path, "r") as file:
    metrics = json.load(file)

day_boundaries = metrics["day_boundaries"]
days = list(day_boundaries.keys())

for i in range(len(days) - 1):
    current_day = days[i]
    next_day = days[i + 1]
    day_boundaries[current_day]["end"] = day_boundaries[next_day]["start"] - 1

last_day = days[-1]
day_boundaries[last_day]["end"] = metrics["latest_height"]
