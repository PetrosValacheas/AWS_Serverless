import csv
import argparse
from datetime import datetime
import pytz

# Define command line arguments
parser = argparse.ArgumentParser(description='Convert datetime in CSV file to UTC.')
parser.add_argument('timezone', type=str, help='Timezone of the CSV file')
parser.add_argument('input_csv', type=str, help='Path to input CSV file')
args = parser.parse_args()

# Load input CSV file
with open(args.input_csv, newline='') as csvfile:
    reader = csv.reader(csvfile)
    rows = [row for row in reader]

# Convert datetime to UTC
timezone = pytz.timezone(args.timezone)
for i in range(1, len(rows)):
    row = rows[i]
    local_dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
    utc_dt = timezone.localize(local_dt).astimezone(pytz.utc)
    rows[i][0] = utc_dt.strftime('%Y-%m-%d %H:%M:%S')

# Save output CSV file
output_csv = args.input_csv.split('.')[0] + '_utc.csv'
with open(output_csv, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(rows)

print(f'Output CSV file saved as {output_csv}')