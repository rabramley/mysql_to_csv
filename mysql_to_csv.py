import pymysql
import csv
import os
from dotenv import load_dotenv

load_dotenv()

def export(table_name):
    db = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_DATABASE'],
    )

    cur = db.cursor()

    sql = f'SELECT * FROM {table_name}'
    csv_file_path = f'output/{table_name}.csv'

    try:
        cur.execute(sql)

        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"')

            csvwriter.writerow([d[0] for d in cur.description])

            for row in cur.fetchall():
                csvwriter.writerow(row)
    finally:
        db.close()

export('emergency_export')
