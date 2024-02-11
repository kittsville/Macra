from datetime import datetime
import psycopg2
import aranet4
import time
import sys

NUM_RETRIES = 10
mac = 'FD:2E:12:DC:0A:85'

con = psycopg2.connect(database="aranet4",
                        host="localhost",
                        user="aranet4",
                        password=sys.argv[1],
                        port="5432")

cur = con.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS measurements(
  timestamp INTEGER,
  temperature REAL,
  humidity INTEGER,
  pressure REAL,
  CO2 INTEGER,
  PRIMARY KEY(timestamp)
)
''')
con.commit()

entry_filter = {}

res = cur.execute('''SELECT timestamp FROM measurements
                      ORDER BY timestamp DESC LIMIT 1''')

if res is not None:
  row = res.fetchone()
  entry_filter['start'] = datetime.utcfromtimestamp(row[0])
  print(f"Getting measurements after: {row[0]}")
else:
  print("Getting all measurements")

for attempt in range(NUM_RETRIES):
  entry_filter['end'] = datetime.now()
  try:
    history = aranet4.client.get_all_records(mac, entry_filter)
    break
  except Exception as e:
    print('attempt', attempt, 'failed, retrying:', e)

data = []
print(f"Fetched {len(history.value)} measurements")
for entry in history.value:
  if entry.co2 < 0:
    continue

  data.append((
    time.mktime(entry.date.timetuple()),
    entry.temperature,
    entry.humidity,
    entry.pressure,
    entry.co2
  ))

cur.executemany(
  'INSERT INTO measurements(timestamp, temperature, humidity, pressure, CO2) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', data)
con.commit()

print("Saved to DB")

con.close()
