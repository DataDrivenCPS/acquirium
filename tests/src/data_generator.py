import datetime
import random
import math
random.seed(42)
time_start = datetime.datetime.strptime("2023-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')


points = [f"point_{i}" for i in range(1,11)]
data = []
header = "Timestamp," + ",".join(points)
data.append(header)
for i in range(365*24):
    row = [time_start.strftime('%Y-%m-%d %H:%M:%S')]
    for p in range(4):
        row.append(str(random.randint(0, 100)))
    for p in range(4):
        row.append(str(50 + 10 * math.sin(2 * math.pi *(time_start.hour * 60 + time_start.minute) * random.random() / 1440)))
    if random.random() < 0.1:
        row.append("OFF")
    else:
        row.append("ON")
    if random.random() < 0.9:
        row.append("OFF")
    else:
        row.append("ON")
    time_start += datetime.timedelta(hours=1)
    data.append(",".join(row))

with open("tests/sample_data.csv", "w") as f:
    f.write("\n".join(data))