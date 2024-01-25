import csv
import json
import xml.etree.ElementTree as ET
from pony.orm import Database, PrimaryKey, db_session, Optional


db = Database()

class DataComb(db.Entity):
    id = PrimaryKey(int, auto=True)
    first_name = Optional(str)
    second_name = Optional(str)
    age = Optional(int)
    sex = Optional(str)
    vehicle_make = Optional(str)
    vehicle_model = Optional(str)
    vehicle_year = Optional(str)
    vehicle_type = Optional(str)
    iban = Optional(str)
    credit_card_number = Optional(str)
    credit_card_security_code = Optional(str)
    credit_card_start_date = Optional(str)
    credit_card_end_date = Optional(str)
    address_main = Optional(str)
    address_city = Optional(str)
    address_postcode = Optional(str)
    retired = Optional(str)
    dependants = Optional(int)
    marital_status = Optional(str)
    salary = Optional(int)
    pension = Optional(int)
    company = Optional(str)
    commute_distance = Optional(float)
    dependants = Optional(int)
    marital_status = Optional(str)
    salary = Optional(int)
    pension = Optional(int)
    company = Optional(str, nullable=True)
    commute_distance = Optional(float)
    

# Connect to the database
db.bind(provider='sqlite', filename='comb_data.db', create_db=True)
db.generate_mapping(create_tables=True)



def read_csv(file_path):
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            yield {
                'first_name': row['First Name'],
                'second_name': row['Second Name'],
                'age': int(row['Age (Years)']),
                'sex': row['Sex'],
                'vehicle_make': row['Vehicle Make'],
                'vehicle_model': row['Vehicle Model'],
                'vehicle_year': row['Vehicle Year'],
                'vehicle_type': row['Vehicle Type'],
            }

# Function to read and extract data from JSON file
def read_json(file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
        for item in data:
            yield {
                'first_name': item.get('firstName', ''),
                'second_name': item.get('lastName', ''),
                'age': item.get('age', None),
                'iban': item.get('iban', ''),
                'credit_card_number': item.get('credit_card_number', ''),
                'credit_card_security_code': item.get('credit_card_security_code', ''),
                'credit_card_start_date': item.get('credit_card_start_date', ''),
                'credit_card_end_date': item.get('credit_card_end_date', ''),
                'address_main': item.get('address_main', ''),
                'address_city': item.get('address_city', ''),
                'address_postcode': item.get('address_postcode', ''),
            }




# Function to read and extract data from XML file
def read_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    for user in root.findall('user'):
        dep_str = user.get('dependants', '0')
        try:
            dependants = int(dep_str)
        except ValueError:
            dependants = None
        commute_distance = user.get('commute_distance', None)

        company = user.get('company')
        if company == ' ' or company == 'N/A':
            company = None
        yield {
            'first_name': user.get('firstName', ''),
            'second_name': user.get('lastName', ''),
            'age': int(user.get('age',0)),
            'sex': user.get('sex', ''),
            'retired': user.get('retired', '').lower() == 'true',
            'dependants': dependants,
            'marital_status': user.get('marital_status', ''),
            'salary': None if user.get('pension') == '0' else int(user.get('pension', None)),    #int(user.get('salary', None)),
            'pension': None if user.get('pension') == '0' else int(user.get('pension', None)),   #int(user.get('pension', None)),
            'company': company,
            'commute_distance': float(commute_distance) if commute_distance != '0' else None,  #float(user.get('commute_distance', None)),
            'address_postcode': user.get('address_postcode', ''),

        }




# Main function to unify data and insert into the database
def main():
    csv_data = read_csv('data.csv')
    json_data = read_json('data.json')
    xml_data = read_xml('data.xml')


    

    comb_data = list(csv_data) + list(json_data) + list(xml_data)
    
    

    # with open('DataCombs.txt', 'w') as f:
    #     f.write(str(comb_data))
    #     f.close
    # Insert unified records into the database
    with db_session:
        for record in comb_data:
            if 'retired' in record:
                record['retired'] = str(record['retired'])
            DataComb(**record)
    

if __name__ == "__main__":  
    main()

