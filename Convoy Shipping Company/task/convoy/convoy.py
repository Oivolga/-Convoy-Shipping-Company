import pandas as pd
import csv
import re
import sqlite3
import json
from xml.dom.minidom import parseString
from dicttoxml import dicttoxml

file_name = input('Input file name\n')
new_name = ''
checked = ''


def create_json():
    if file_name.endswith('.s3db'):
        base_name = file_name
    else:
        base_name = checked.replace('[CHECKED].csv', '.s3db')
    conn = sqlite3.connect(base_name)
    cursor = conn.cursor()
    cursor.execute("SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy WHERE score > 3;")
    row_headers = [x[0] for x in cursor.description]
    json_results = cursor.fetchall()
    cursor.execute("SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy WHERE score <= 3;")
    xml_results = cursor.fetchall()
    conn.close()
    json_name = base_name.replace('.s3db', '.json')
    json_data = []
    xml_data = []
    count = 0
    xml_count = 0
    for result in json_results:
        json_data.append(dict(zip(row_headers, result)))
        count += 1
    json_output = {'convoy': json_data}
    with open(f'{json_name}', "w") as json_file:
        json.dump(json_output, json_file)
    if count == 1:
        out = f'1 vehicle was saved into {json_name}'
    else:
        out = f'{count} vehicles were saved into {json_name}'
    print(out)

    for result in xml_results:
        xml_data.append(dict(zip(row_headers, result)))
        xml_count += 1

    xml_name = json_name.replace('.json', '.xml')
    xml = dicttoxml(xml_data, attr_type=False, custom_root='convoy', item_func=lambda x: 'vehicle')
    xml_file = open(f"{xml_name}", "w")
    if xml_count == 0:
        xml_file.write('<convoy></convoy>')
    else:
        xml_file.write((parseString(xml).toprettyxml()).replace('<?xml version="1.0" ?>', ''))
    xml_file.close()
    if xml_count == 1:
        out = f'1 vehicle was saved into {xml_name}'
    else:
        out = f'{xml_count} vehicles were saved into {xml_name}'
    print(out)


def create_db():
    base_name = checked.replace('[CHECKED].csv', '.s3db')
    conn = sqlite3.connect(base_name)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS convoy(
        vehicle_id INTEGER PRIMARY KEY NOT NULL,
        engine_capacity INTEGER NOT NULL,
        fuel_consumption INTEGER NOT NULL,
        maximum_load INTEGER NOT NULL,
        score INTEGER NOT NULL);""")
    conn.commit()
    check_data = [line for line in csv.reader(open(checked))]
    check_data[0].append('score')

    for i in range(1, len(check_data)):
        fuel = (450 * int(check_data[i][2]) / 100)
        pitstop = (fuel / int(check_data[i][1]))
        score = 0
        if pitstop < 1:
            score += 2
        elif 1 < pitstop < 2:
            score += 1

        if fuel <= 230:
            score += 2
        else:
            score += 1

        if int(check_data[i][3]) >= 20:
            score += 2

        check_data[i].append(str(score))

    db_row = 0
    for j in range(1, len(check_data)):
        row = tuple(check_data[j])
        cursor.execute(
            "INSERT OR REPLACE INTO convoy(vehicle_id, engine_capacity, fuel_consumption, maximum_load, score) VALUES ("
            "?, ?, ?, ?, ?)", row)
        db_row += 1
        conn.commit()
    if db_row == 1:
        print(f'1 record was inserted into {base_name}')
    else:
        print(f'{db_row} records were inserted into {base_name}')

    conn.commit()
    conn.close()
    create_json()


def fix_data():
    pre_data = [line for line in csv.reader(open(new_name))]
    data = pre_data.copy()

    count = 0
    for y in range(1, len(data)):
        for x in range(0, len(data[y])):
            num = len(data[y][x])
            data[y][x] = re.sub('[a-z._]', '', data[y][x]).strip()
            if num != len(data[y][x]):
                count += 1

    with open(f"{checked}", "w", encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter=",", lineterminator="\n")
        for i in data:
            file_writer.writerow(i)

    print(f'{count} cells were corrected in {checked}')
    create_db()


if file_name.endswith('.s3db'):
    create_json()
elif file_name.endswith('[CHECKED].csv'):
    checked = file_name
    create_db()
elif file_name.endswith('xlsx'):
    my_df = pd.read_excel(file_name, sheet_name='Vehicles', dtype=str)
    new_name = file_name.replace('.xlsx', '.csv')
    checked = file_name.replace('.xlsx', '[CHECKED].csv')
    my_df.to_csv(new_name, index=None)
    if my_df.shape[0] == 1:
        output = 'line was'
    else:
        output = 'lines were'
    print(f'{my_df.shape[0]} {output} added to {new_name}')
    fix_data()
elif file_name.endswith('.csv'):
    new_name = file_name
    checked = file_name.replace('.csv', '[CHECKED].csv')
    fix_data()
