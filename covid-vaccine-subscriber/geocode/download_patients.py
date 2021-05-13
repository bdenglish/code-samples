from csv import DictReader
from datetime import date
from urllib.parse import quote
import argparse
import json
import logging
import os.path
import re
import sys
from os import getenv

# from IPython import embed
import requests
import pandas as pd
from geopy import distance
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

SCOPES = getenv('GOOGLE_SHEETS_SCOPE')
SAMPLE_SPREADSHEET_ID = getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
SAMPLE_RANGE_NAME = getenv('GOOGLE_SHEETS_RANGE', 'Ben!A:S')
BASE_DIR = '/opt/app-root'

formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('geocode.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--pharmacies", default="/inputs/pharmacies.csv")
parser.add_argument("-a", "--appointments", default="/inputs/appointments.csv")
parser.add_argument("-o", "--output", default=f"/output/new_appointments_{date.today()}.json")
args = parser.parse_args()


def persist_to_file(file_name):
    def decorator(original_func):

        try:
            cache = json.load(open(file_name, 'r'))
        except (IOError, ValueError):
            cache = {}

        def new_func(param):
            if param not in cache:
                cache[param] = original_func(param)
                json.dump(cache, open(file_name, 'w'))
            return cache[param]

        return new_func

    return decorator


def get_mapbox_url(address):
    return f'https://api.mapbox.com/geocoding/v5/mapbox.places/{quote(address)}.json?' \
           f'country=us&types=address&autocomplete=false&proximity=-75.4,40.0&' \
           f'access_token={getenv("MAPBOX_TOKEN")}'


@persist_to_file('/Users/ben/git/covid-vaccine-subscriber/output/mapbox.json')
def get_mapbox_json(address):
    response = requests.get(get_mapbox_url(address))
    return json.loads(response.content)


def fmt_dob(dob):
    m, d, y = dob.strip().split('/')
    if y[:2] != '19' and y[:2] != '20':
        y = f'19{y[2:]}'
    return f'{int(m):02d}{int(d):02d}{y}'


def fmt_phone(phone):
    return re.sub('[^\d]', '', phone)


def get_patient_info_data_frame():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(f'{BASE_DIR}/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
        return pd.DataFrame()
    else:
        columns = values[0]
        print(columns)
        return pd.DataFrame(values[1:], columns=columns)


def parse_times_of_day(s):
    if 'any' in s.lower() or s == '':
        return list(range(24))
    times = s.split(',')

    hours = []
    for t in times:
        if t.strip().startswith("10AM"):
            hours.extend(list(range(10, 13, 1)))
        elif t.strip().startswith("1PM"):
            hours.extend(list(range(13, 16, 1)))
        elif t.strip().startswith("4PM"):
            hours.extend(list(range(16, 19, 1)))

    return hours


def parse_dow(s):
    if 'any' in s.lower() or s == '':
        return list(range(7))

    days = s.split(',')
    dow = []
    for d in days:
        d = d.strip()
        if d == 'Monday':
            dow.append(0)
        elif d == 'Tuesday':
            dow.append(1)
        elif d == 'Wednesday':
            dow.append(2)
        elif d == 'Thursday':
            dow.append(3)
        elif d == 'Friday':
            dow.append(4)
        elif d == 'Saturday':
            dow.append(5)
        elif d == 'Sunday':
            dow.append(6)

    return dow


if __name__ == '__main__':
    df = get_patient_info_data_frame()
    appointments = df.to_dict(orient='records')
    with open(args.pharmacies) as f:
        pharmacies = list(DictReader(f))

    appointments_json = []
    for r in appointments:
        if r.get('confirmed') != 'Yes':
            continue
        keep_zips_with_dist = []
        keep_zips = []
        address = f'{r["street"]}, {r["city"]}, {r["state"]}, {r["zip_code"]}'
        max_distance = int(r['max_distance'].split()[0])
        person_long, person_lat = get_mapbox_json(address)['features'][0]['center']
        person_loc = (person_lat, person_long)
        for pharm in pharmacies:
            if 'New Jersey' in pharm['Address'] and r['state'] == 'PA':
                continue
            zipcode = pharm['Zipcode']
            mapbox = get_mapbox_json(pharm['Address'])
            pharm_long, pharm_lat = mapbox['features'][0]['center']
            pharm_loc = (pharm_lat, pharm_long)
            dist = distance.great_circle(person_loc, pharm_loc).miles
            # linear regression on error based on test data below
            error = -0.6546115857280128 + 0.3232529983374981 * dist
            estimated_dist = dist + error
            if estimated_dist <= max_distance:
                logger.info(
                    f'dist: {estimated_dist:.2f}, {r["first_name"]}: {address}, {person_loc}; '
                    f'pharm: {pharm["Address"]}, {pharm_loc}')
                keep_zips_with_dist.append((dist, zipcode))

        keep_zips_with_dist.sort()
        for dist, zipcode in keep_zips_with_dist:
            if zipcode not in keep_zips:
                keep_zips.append(zipcode)
        if not keep_zips:
            continue
        appointments_json.append({
            'signup_timestamp': r.get('timestamp'),
            'first_name': r['first_name'].strip(), 'last_name': r['last_name'].strip(),
            'dob': fmt_dob(r['dob']), 'phone': fmt_phone(r['phone']),
            'address': r['street'].strip(), 'city': r['city'].strip(), 'state': r['state'].strip(),
            'zip': r['zip_code'].strip(), 'email': r['email'].strip(), 'contact_preference': r['contact_preference'],
            'cell_phone': r.get('is_cell_phone'), 'times_of_day': parse_times_of_day(r.get('times_of_day')),
            'days_of_week': parse_dow(r.get('days_of_week', 'any')), 'notes': r.get('notes'),
            'age': r.get('age'),
            'target_zip_codes': keep_zips, "min_date_offset": 0,
        })

    list_1 = [j for i, j in enumerate(appointments_json) if i % 2 == 0]
    list_2 = [j for i, j in enumerate(appointments_json) if i % 2 != 0]
    with open(f'{args.output}/1/patients.json', 'w') as f:
        logger.info(f'Writing {len(list_1)} patients to {args.output}/1/patients.json')
        json.dump(list_1, f, indent=2)
    with open(f'{args.output}/2/patients.json', 'w') as f:
        logger.info(f'Writing {len(list_2)} patients to {args.output}/2/patients.json')
        json.dump(list_2, f, indent=2)
