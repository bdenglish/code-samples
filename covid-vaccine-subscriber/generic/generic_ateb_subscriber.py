from datetime import datetime, timedelta
from pathlib import Path
import argparse
import json
import logging
import os
import random
import re
import sys
import time

from cachetools import TTLCache
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import fasteners

ZIP_CACHE = TTLCache(maxsize=10000, ttl=60) # 1 minute cache

def swap(l, p=0.5, inplace=False):
    """Swap adjacent elements of l with probability p."""
    l2 = l
    if not inplace:
        l2 = l.copy()
    idxs = list(range(len(l) - 1))
    random.shuffle(idxs)
    for i in idxs:
        if random.uniform(0, 1) < p:
            a = l2[i]
            l2[i] = l2[i + 1]
            l2[i + 1] = a
    return l2


def save_locations(schedule_containers):
    n = f'{datetime.now().strftime("%Y-%m-%d_%H-%M")[:-1]}0-00'
    if not os.path.exists(f'{output_path}{bot_id}_locations_{n}.json'):
        with open(f'{output_path}{bot_id}_locations_{n}.json', 'w') as f:
            json.dump(
                [{'search_datetime': datetime.now().isoformat(),
                  'address': d.get('address'),
                  'date_avail': d.get('date_avail'),
                  'hours_avail': d.get('hours_avail')} for d in schedule_containers],
                f,
                default=str,
                indent=2)


def get_hours_from(n, t=11):
    yest = n - timedelta(1)
    yest = datetime(yest.year, yest.month, yest.day, t, 0, 0)
    tomm = n + timedelta(1)
    tomm = datetime(tomm.year, tomm.month, tomm.day, t, 0, 0)
    today = datetime(n.year, n.month, n.day, t, 0, 0)
    prev_t = (n - yest).total_seconds() / 60 ** 2
    now_t = abs((n - today).total_seconds() / 60 ** 2)
    next_t = (tomm - n).total_seconds() / 60 ** 2
    return min([prev_t, now_t, next_t])


def time_to_sleep(hours):
    return (max(0, hours - 8) * 855) + 180


def get_last_submit_button(browser, button_text='Search'):
    buttons = browser.find_elements_by_class_name("ac-pushButton.style-default")
    button = buttons[-1]
    for b in buttons[::-1][:20]:
        if b.text == button_text:
            button = b

    return button


def wait_for_more_buttons(browser, _buttons, max_wait=200):
    logger.info(f"wait for more than {len(_buttons)} buttons to appear")
    total_sleep = 0
    while len(browser.find_elements_by_tag_name("button")) <= len(_buttons) and total_sleep < max_wait:
        logger.debug(f'{len(browser.find_elements_by_tag_name("button"))} buttons found, sleeping')
        time.sleep(0.5)
        total_sleep += 0.5
    return browser.find_elements_by_tag_name("button")


def wait_for_more_spans(browser, _spans, max_wait=300):
    logger.info(f"wait for more than {len(_spans)} buttons to appear")
    total_sleep = 0
    spans_found = []
    while len(spans_found) <= len(_spans) and total_sleep < max_wait:
        spans_found = browser.find_elements_by_tag_name("span")
        print(f'{len(spans_found)} spans found, sleeping')
        time.sleep(0.5)
        total_sleep += 0.5
    return spans_found


def extract_zip(_address):
    zip_code_matches = re.findall(r'PA\s(\d{5})', _address)
    if len(zip_code_matches) > 0:
        return zip_code_matches[-1]
    else:
        logger.warning(f'could not find a zip code match in {_address}')
        return '99999'


def find_input_box_header(browser, header_text):
    all_divs = browser.find_elements_by_class_name("ac-textBlock")
    # patient_info_y = [d for d in all_divs if header_text in d.text][-1].rect.get('y', 56)
    patient_info_y = 1e7
    for d in all_divs[::-1][:20]:
        if header_text in d.text:
            patient_info_y = d.rect.get('y', 56)
            logger.info(f'found start of the {header_text} section')
            break

    if patient_info_y == 1e7:
        logger.error(f'could not find the start of the {header_text} section')

    text_boxes = browser.find_elements_by_class_name("ac-input.ac-textInput")
    for i, tb in enumerate(text_boxes):
        if patient_info_y < tb.rect.get('y', -1):
            logger.info(f'patient info starts at textInput: {i})')
            return i, text_boxes

    return 0, None


def screenshot_and_save(browser, id, save_html=True):
    _now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    browser.save_screenshot(f'{output_path}/{id}_{_now}.png')
    if save_html:
        with open(f"{output_path}/{id}_{_now}.html", "w") as f:
            f.write(browser.page_source)


def wait_for_next_screen(browser, n, max_wait=120, s=0.5):
    total_wait = 0
    while total_wait <= max_wait:
        button_class = "ac-pushButton.style-default"
        buttons = browser.find_elements_by_class_name(button_class)
        if len(buttons) > n:
            return buttons
        logger.debug(f'sleeping for: {s}s total_wait: {total_wait}, max_wait: {max_wait}, {len(buttons)}')
        total_wait += s
        time.sleep(s)
        if browser.find_elements_by_tag_name('button')[-3].text == 'Try Again':
            logger.warning(f'received try again message, no appointments available')
            return None


def load_search_screen(browser):
    try:
        WebDriverWait(browser, 60).until(EC.presence_of_element_located((By.NAME, 'actionType')))
    except TimeoutException as e:
        logger.info('initial page did not load')
        return None

    options = browser.find_elements_by_class_name("ac-input.ac-choiceSetInput-expanded")[-1]
    if 'Schedule a new appointment' not in options.text:
        logger.info(options.text)
        logger.info('no schedule appointment button')
        return None

    for l in browser.find_elements_by_tag_name('label'):
        if l.text == 'Schedule a new appointment':
            l.click()
            break

    buttons = wait_for_next_screen(browser, n=0)
    if buttons is None:
        logger.info('wait_for_next_screen returned None')
        return None

    buttons[-1].click()
    buttons = wait_for_next_screen(browser, len(buttons))
    return buttons


def parse_container_text(s):
    rows = s.split('\n')
    d = {'address': rows[0],
         'zip': rows[0].split(f', PA ')[-1],
         'phone': rows[3].split(':')[-1].strip(),
         'date_avail': datetime.strptime(rows[5].split(':')[-1].strip(), '%m/%d/%Y').date()}
    d['day_of_week'] = d['date_avail'].weekday()
    return d


def look_for_appointments(patient,
                          chat_bot_url='file:///opt/app-root/weis_chat_bot_source.html',
                          selenium_grid=True,
                          grid_url='http://127.0.0.1:4444'):
    target_zip_codes = patient.get('target_zip_codes', ['99999'])
    in_cache = True
    ZIP_CACHE.expire()
    for code in target_zip_codes:
        if code not in ZIP_CACHE:
            in_cache = False
    if in_cache:
        logger.info(f'{target_zip_codes} in ZIP_CACHE, skipping')
        return {'browser': None}
    state = patient.get('state_abbr', patient.get('state', 'PA'))
    not_dow = patient.get('not_dow', -1)
    only_dow = patient.get('only_dow', -1)

    min_date = f'{(datetime.now() + timedelta(day_offset)).date()}'
    logger.info(f'loading {chat_bot_url}')

    if selenium_grid:
        browser = webdriver.Remote(
            command_executor=grid_url,
            desired_capabilities=DesiredCapabilities.FIREFOX)
    else:
        options = webdriver.FirefoxOptions()
        browser = webdriver.Firefox(options=options)

    browser.set_window_size(1400, 900)
    browser.get(chat_bot_url)
    logger.info(f"waiting for initial page name: {chat_bot_url}")
    buttons = load_search_screen(browser)
    if buttons is None:
        return {'browser': browser, 'appointments': False}

    def match_results_to_target(_target_zip_codes, schedule_buttons):
        target_zips_set = set(_target_zip_codes)
        button_zips_set = set([c.get("zip") for c in schedule_buttons if c.get("zip")])
        matching_zips = target_zips_set & button_zips_set
        if not matching_zips:
            return False
        filtered_target_zips = [z for z in _target_zip_codes if z in matching_zips]
        matching_buttons = {c.get("zip"): c for c in schedule_buttons if c.get("zip") in target_zips_set}
        ordered_buttons = [matching_buttons[z] for z in filtered_target_zips]
        for c in ordered_buttons:
            if c.get("day_of_week") in patient.get('days_of_week', range(7)):
                logger.info(f'found matching appointment at: {c.get("address")} on dow: {c.get("day_of_week")}'
                            f'patient day_of_week preference: {patient.get("days_of_week")}')
                return {'submit_button': c.get('button'), 'address': c.get('address')}
        return False

    def parse_search_results(browser):

        containers = browser.find_elements_by_class_name("ac-container")
        if containers[-1].text == 'Search':
            return False

        start_container = 0
        for i in range(15, 32):
            if containers[i].text == 'Search':
                start_container = i + 4
                break

        if start_container == 0:
            logger.error(f'could not find the start container')
            return False
        else:
            logger.info(f'the start container was: {start_container}')

        containers = containers[start_container:]
        logger.info(f'scanning {len(containers)} containers for appointment info')
        schedule_containers = []
        start_time = time.time()
        i = 1
        while i < len(containers) - 1:
            if containers[i].size.get('height', 0) >= 100.0:
                container_text = containers[i].text
                schedule_containers.append(dict(button=containers[i + 2], **parse_container_text(container_text)))
                i += 5
            else:
                i += 1
        logger.info(f'it took {time.time() - start_time:.2f} to parse the appointment containers while loop')
        logger.info(f'parsed {len(schedule_containers)} appointments')
        if len(schedule_containers) > 0:
            try:
                save_locations(schedule_containers)
            except Exception as e:
                logger.error(e)

        return schedule_containers

    def search_whole_state(browser, _state, buttons, _zip_code):
        logger.info(f"enter start date for search: {min_date}")
        date_selection = browser.find_element_by_class_name("ac-input.ac-dateInput")
        date_selection.send_keys(min_date)
        # find the zip code box
        zip_code_search = browser.find_elements_by_class_name("ac-input.ac-textInput")[-1]
        zip_code_search.send_keys(_zip_code)
        # find the distance box
        distance_box = browser.find_elements_by_class_name("ac-input.ac-multichoiceInput.ac-choiceSetInput-compact")[-1]
        distance_box.send_keys("1")
        distance_box.send_keys(Keys.DOWN * 3)
        time.sleep(0.1)

        buttons[-1].click()
        buttons = wait_for_next_screen(browser, len(buttons), max_wait=45)
        if buttons is None:
            return False
        sb = parse_search_results(browser)
        if not sb:
            return False
        return match_results_to_target(target_zip_codes, sb)

    state_search = browser.find_element_by_class_name("ac-input.ac-multichoiceInput.ac-choiceSetInput-compact")
    bot_state = bot_id.split('_')[-1]
    logger.info(f'Sending state: {bot_state}')
    state_search.send_keys(bot_state)
    result = search_whole_state(browser,
                                state,
                                browser.find_elements_by_class_name("ac-pushButton.style-default"),
                                _zip_code=patient.get("zip"))
    if result:
        return dict(browser=browser, **result)
    else:
        logger.info('state_search: No result')
        for code in target_zip_codes:
            ZIP_CACHE[code] = True

    return {'browser': browser}


def find_and_click_new_appointment(browser):
    _buttons = browser.find_elements_by_class_name("ac-textBlock")
    bt = [b.text for b in _buttons if "new appointment" in b.text]
    if len(bt) > 0:
        click_schedule_appointment_button(browser)
        return True
    else:
        logger.info('no new appointment radio button')
        return False


def click_schedule_appointment_button(browser):
    logger.info("select schedule vaccine button and submit")
    radio_buttons = browser.find_elements_by_name('actionType')
    logger.info(f'{len(radio_buttons)} radio buttonsfound')
    radio_buttons[0].click()
    submit_button_class = "ac-actionSet"
    submit_button = browser.find_element_by_class_name(submit_button_class)
    submit_button.click()


def complete_patient_info(browser, buttons, first_name, last_name, dob, phone):
    # complete patient info
    logger.info('filling out patient info')
    i, text_boxes = find_input_box_header(browser, header_text='Patient Info')
    if i == 0:
        logger.error(f'could not find the start of the patient info section')
        if browser is not None:
            browser.quit()
        return False, 'failed to load patient info'
    text_boxes[i].click()
    text_boxes[i].send_keys(first_name)
    text_boxes[i + 2].click()
    text_boxes[i + 2].send_keys(last_name + Keys.TAB + dob[:2] + Keys.TAB + dob[2:4] + Keys.TAB + dob[4:])
    # date_box = browser.find_elements_by_class_name("ac-input.ac-dateInput")[-1]
    browser.find_elements_by_class_name("ac-input.ac-toggleInput")[-1].click()
    text_boxes[i + 3].click()
    text_boxes[i + 3].send_keys(phone)
    time.sleep(1)

    # submit patient info
    logger.info('submitting patient info')
    submit_buttons = browser.find_elements_by_class_name("ac-pushButton.style-default.primary.style-positive")
    submit_buttons[-1].click()

    return wait_for_more_buttons(browser, buttons, max_wait=45)


def complete_contact_info(browser, buttons, address, city, state, zip_code, email):
    # fill out contact info
    logger.info('filling out contact info')

    i, text_boxes = find_input_box_header(browser, header_text='Patient Contact Info')
    if i == 0:
        logger.error(f'could not find the start of the patient contact info section')
        if browser is not None:
            browser.quit()
        return False, 'failed to load patient contact info'
    text_boxes[i].click()
    text_boxes[i].send_keys(address)
    text_boxes[i + 1].click()
    text_boxes[i + 1].send_keys(city)
    state_box = browser.find_elements_by_class_name("ac-input.ac-multichoiceInput.ac-choiceSetInput-compact")[-1]
    state_box.send_keys(state[:3])
    text_boxes[i + 2].click()
    text_boxes[i + 2].send_keys(zip_code)
    text_boxes[i + 4].click()
    text_boxes[i + 4].send_keys(email)
    time.sleep(3)

    logger.info('submitting contact info')
    submit_buttons = browser.find_elements_by_class_name("ac-pushButton.style-default.primary.style-positive")
    submit_buttons[-1].click()

    return wait_for_more_buttons(browser, buttons, max_wait=45)


def schedule_appointment(appointment_result, patient):
    try:
        browser = appointment_result.get('browser')
        submit_button = appointment_result.get('submit_button')
        appointment_address = appointment_result.get('address')
        logger.info(json.dumps(patient, indent=2))
        # patient info
        first_name = patient.get('first_name', 'X')
        last_name = patient.get('last_name', 'X')
        dob = patient.get('dob', '01011950')
        phone = patient.get('phone', '1234567780')
        address = patient.get('address', '100 Main Street')
        city = patient.get('city', 'North Wales')
        state = patient.get('state', 'PA')
        zip_code = patient.get('zip', '99999')
        email = patient.get('email', 'name@email.com')
        min_date_offset = patient.get('min_date_offset', 0)
        run_id = f'{first_name}_{last_name}'

        buttons = browser.find_elements_by_tag_name("button")
        submit_button.click()

        logger.info(f'found a matching appointment for {first_name} {last_name} at {appointment_address}')
        buttons = wait_for_more_buttons(browser, buttons, max_wait=40)

        # agree to terms
        logging.info('clicking check box to agree to terms and submitting')
        browser.find_elements_by_class_name("ac-input.ac-toggleInput")[-1].click()
        browser.find_elements_by_class_name("ac-pushButton.style-default.primary.style-positive")[-1].click()

        buttons = wait_for_more_buttons(browser, buttons)

        buttons = complete_patient_info(browser, buttons, first_name, last_name, dob, phone)
        buttons = complete_contact_info(browser, buttons, address, city, state, zip_code, email)

        logger.info('click and submit gender')
        browser.find_elements_by_name("gender")[-1].click()
        submit_buttons = browser.find_elements_by_class_name("ac-pushButton.style-default.primary.style-positive")
        submit_buttons[-1].click()

        buttons = wait_for_more_buttons(browser, buttons)
        logger.info('finding cash selection button')
        spans = browser.find_elements_by_tag_name("span")
        found_cash = False
        for span in spans[::-1][:20]:
            if span.text == 'Cash':
                found_cash = True
                span.click()
                break

        if found_cash:
            buttons = wait_for_more_buttons(browser, buttons)
        logger.info('finding and selecting random available time')
        screenshot_and_save(browser, id=run_id)

        def time_to_hour(_s):
            _s = _s.lower()
            h = int(_s.split(':')[0])
            if 'pm' in _s and h != 12:
                h += 12
            return h

        if old_style_time:
            logging.warning(f'using old butten based time selection')
            spans = browser.find_elements_by_tag_name("span")
            times = [{'element': s, 'hour': time_to_hour(s.text.strip()), 'text': s.text.strip()}
                     for s in spans if ":" in s.text and '-' not in s.text and ("AM" in s.text or "PM" in s.text)]
            if len(times) > 0:
                logging.info(f'available times: {[t["hour"] + " -- " + t["text"] for t in times]}')
                times = [t for t in times if t.get('hour') in patient.get('times_of_day', range(24)) or
                         not patient.get('times_of_day')]
                logging.info(f'available times (after removing based on prefs: '
                             f'{[str(t["hour"]) + " -- " + t["text"] for t in times]}')
                if reverse_times:
                    times.reverse()
                elif swap_times:
                    swap(times, inplace=True)
                elif forward_times:
                    pass
                else:
                    random.shuffle(times)
                times[0]['element'].click()
                logging.info(f'attempting to book time: {times[0]["text"]}')
        else:
            logging.warning(f'using new combo box based time selection')
            times = [{'element': s, 'hour': time_to_hour(s.text.strip()), 'text': s.text.strip()}
                     for s in browser.find_elements_by_tag_name('option')
                     if ":" in s.text and '-' not in s.text and ("AM" in s.text or "PM" in s.text)]
            logging.info(f'available times: {[str(t["hour"]) + " -- " + t["text"] for t in times]}')
            times = [t for t in times if t.get('hour') in patient.get('times_of_day', range(24)) or
                     not patient.get('times_of_day')]
            logging.info(f'available times (after removing based on prefs: '
                         f'{[str(t["hour"]) + " -- " + t["text"] for t in times]}')
            if reverse_times:
                times.reverse()
            elif swap_times:
                swap(times, inplace=True)
            elif forward_times:
                pass
            else:
                random.shuffle(times)
            times[0]['element'].click()
            logging.info(f'attempting to book time: {times[0]["text"]}')
            submit_buttons = browser.find_elements_by_class_name("ac-pushButton.style-default")
            submit_buttons[-1].click()

        buttons = wait_for_more_buttons(browser, buttons)

        to_click = 'Yes' if live_appt else 'No'
        spans = browser.find_elements_by_tag_name("span")
        # [s for s in browser.find_elements_by_tag_name("span") if s.text == to_click][-1].click()
        for s in browser.find_elements_by_tag_name("span")[::-1][:20]:
            if s.text == to_click:
                s.click()
                break
        logging.info(f'clicked {to_click}')
        if to_click == 'No':
            if browser is not None:
                browser.quit()
            return False, 'clicked No'
        old_len = len(browser.find_elements_by_class_name("ac-richTextBlock"))
        screenshot_and_save(browser, id=run_id)

        max_wait = 120
        new_output = browser.find_elements_by_class_name("ac-richTextBlock")
        start = time.time()
        while len(new_output) <= old_len and time.time() - start < max_wait:
            logger.info(f'Waiting for success message {len(new_output)} <= {old_len}')
            time.sleep(1)
            new_output = browser.find_elements_by_class_name("ac-richTextBlock")
        if not new_output[-1].text.count('Confirmation Number'):
            screenshot_and_save(browser, id=run_id)
            return False, 'no confirmation'
        message = [f'Hi {first_name.title()} -', 'Here is your COVID-19 Vaccination Appointment information:',
                   f'{bot_id.split("_")[0].title()} Pharmacy'] + [s.text for s in new_output[-6:]]
        logger.warning('\n'.join(message))
        screenshot_and_save(browser, id=run_id, save_html=False)
        try:
            confirmation = {'signup_timestamp': patient.get('signup_timestamp', '--'),
                            'vaccine_group': patient.get('vaccine_group', '1A'),
                            'first_name': first_name.title(),
                            'last_name': last_name.title(),
                            'dob': f'{int(dob[:2])}/{int(dob[2:4])}/{int(dob[4:])}',
                            'phone': phone,
                            'email': email,
                            'address': address,
                            'city': city,
                            'state': state,
                            'zip_code': zip_code,
                            'max_distance': patient.get('max_distance'),
                            'cell_phone': patient.get('is_cell_phone'),
                            'contact_preference': patient.get("contact_preference", "unknown"),
                            'times_of_day': patient.get('times_of_day'),
                            'days_of_week': patient.get('days_of_week'),
                            'confirmed': 'Yes',
                            'notes': patient.get('notes'),
                            'age': patient.get('age'),
                            'appointment_info': '\n'.join(message),
                            'appointment_pharmacy': bot_id.split('_')[0],
                            'appointment_address': message[-6].split(':')[-1].strip(),
                            'appointment_phone': message[-5].split(':')[-1].strip(),
                            'appointment_date': message[-4].split(':')[-1].strip(),
                            'appointment_time': ':'.join(message[-3].split(':')[-2:]).strip(),
                            'confirmation_number': message[-1].split()[-1]
                            }
            with open(
                    f'{output_path}/confirmation_{datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")}_{first_name.strip().replace(" ", "_")}_{last_name.strip().replace(" ", "_")}.json',
                    'w') as f:
                json.dump(confirmation, f, indent=2)
        except Exception as e:
            logger.error(f'error writing confirmation')
            logger.error(e)

        return True, 'Success'

    except TimeoutException as te:
        msg = f'timed out waiting for element to become visible'
        logger.error(msg)
        if browser is not None:
            browser.quit()
        return False, msg
    except Exception as e:
        msg = str(e)
        logger.error(msg)
        if browser is not None:
            browser.quit()
        return False, msg


def accumulate_target_zip_codes(appointments):
    target_zip_codes = []
    for appointment in appointments:
        target_zip_codes.extend(appointment.get("target_zip_codes"))

    return list(set(target_zip_codes))


def read_patients_file(patients_file):
    p = []
    while not p:
        try:
            with open(patients_file, 'r') as f:
                p = json.load(f)
                return p
        except json.JSONDecodeError as e:
            logger.error(e)
            time.sleep(1)
        except Exception as e:
            logger.error(e)
            time.sleep(0.5)


def save_patients_file(patients, patients_file):
    """Save updates after each iteration."""
    # Using a tmp file and then moving it will lower the chance of a race condition
    with open(f'{patients_file}.tmp', 'w') as f:
        json.dump(patients, f, indent=4)
    os.replace(f'{patients_file}.tmp', patients_file)


@fasteners.interprocess_locked('/opt/app-root/input/patients.lock')
def loop_through_patients(patients_file, bot_url, selenium_grid, grid_url):
    patients = [p for p in read_patients_file(patients_file) if not p.get('success', False)]
    logger.info(f'looking for {len(patients)} appointments')
    for patient in swap(patients):
        target_zips = patient.get('target_zip_codes')
        if not patient.get('success', False):
            appt_result = look_for_appointments(patient, bot_url, selenium_grid, grid_url)
            if appt_result.get('appointments') is False:
                logger.info(f'No appointments right now')
                if appt_result.get('browser') is not None:
                    appt_result.get('browser').quit()
                break
            if appt_result.get('submit_button') is not None:
                success, response = schedule_appointment(appt_result, patient)
                patient['success'] = success
                if success:
                    logger.info(f'success for: {patient.get("first_name")} {patient.get("last_name")}')
                    save_patients_file(patients, patients_file)
                    if appt_result.get('browser') is not None:
                        appt_result.get('browser').quit()
                else:
                    logger.info(f'failed processing: {patient.get("first_name")}')
            else:
                patient['success'] = False
                logger.info(
                    f'unable to find an appointment for {patient.get("first_name")} '
                    f'for target zip codes {patient.get("target_zip_codes")}')
                if appt_result.get('browser') is not None:
                    appt_result.get('browser').quit()


def run(patients_file, bot_url, selenium_grid, grid_url):
    max_sleep = 2400
    while [p for p in read_patients_file(patients_file) if not p.get('success', False)]:
        try:
            loop_through_patients(patients_file, bot_url, selenium_grid, grid_url)
            n = datetime.utcnow()
            if n.weekday() in run_days:
                hours = get_hours_from(n)
                max_sleep = time_to_sleep(hours)
        except Exception as e:
            logger.error(e)
        sleep_for = random.randint(int(max_sleep / 3), int(max_sleep))
        logger.info(f'sleeping for {sleep_for / 60:.2f} minutes')
        time.sleep(sleep_for)

    logger.info(f'all appointments succeeded!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--patients_file", type=str, help="path to patients json file",
                        default='../input/patients.json')
    parser.add_argument("--bot_url", type=str, help="file:/// path to a specific bot_url",
                        default='file:///opt/app-root/weis_pa.html')
    parser.add_argument("--output_path", type=str, help="where to output logs and screenshots",
                        default='../output/')
    parser.add_argument('--no_selenium_grid', dest='use_grid', action='store_false', default=True)
    parser.add_argument("--grid_url", type=str, help="e.g. http://127.0.0.1:4444",
                        default='http://192.168.1.7:4444')
    parser.add_argument("--log_level", type=str, help="ERROR/WARNING/INFO/DEBUG", default='INFO')
    parser.add_argument('--live', dest='live_appt', action='store_true', default=False,
                        help='pass this parameter to book live appointments, this is a fail safe for testing')
    parser.add_argument('--day_offset', dest='day_offset', default="2",
                        help='minimum date to search for appointments = now() + timedelta(days=day_offset)')
    parser.add_argument('--old_style_time', dest='old_style_time', action='store_true', default=False)
    parser.add_argument('--reverse_times', dest='reverse_times', action='store_true', default=False)
    parser.add_argument('--forward_times', dest='forward_times', action='store_true', default=False)
    parser.add_argument('--swap_times', dest='swap_times', action='store_true', default=False)
    args = parser.parse_args()

    for arg in vars(args):
        print(f'{arg} - {getattr(args, arg)}')

    patients_file = args.patients_file
    bot_url = args.bot_url
    output_path = args.output_path
    use_grid = args.use_grid
    grid_url = args.grid_url
    log_level_str = args.log_level
    bot_id = bot_url.split('/')[-1].split('.')[0]
    run_days = {0, 2, 4, 5}
    if bot_id.count('wegmans'):
        run_days = {1, 4}
    live_appt = args.live_appt
    day_offset = int(args.day_offset)
    old_style_time = args.old_style_time
    reverse_times = args.reverse_times
    forward_times = args.forward_times
    swap_times = args.swap_times

    Path(output_path).mkdir(parents=True, exist_ok=True)

    if not live_appt:
        print('************** DEBUG MODE **************')
        print('************** DEBUG MODE **************')
        print('************** DEBUG MODE **************')
        print('************** DEBUG MODE **************')
        print('************** DEBUG MODE **************')

    if log_level_str == 'ERROR':
        log_level = 40
    elif log_level_str == 'WARNING':
        log_level = 30
    elif log_level_str == 'INFO':
        log_level = 20
    elif log_level_str == 'DEBUG':
        log_level = 10
    else:
        log_level = 20

    log_file = f'{output_path}{bot_id}_{datetime.utcnow().date()}.log'
    print(f'log_file: {log_file}')

    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(funcName)s  | %(message)s')
    logger = logging.getLogger()
    logger.setLevel(log_level)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f'bot id: {bot_id}')
    logger.info(f'appointments json file: {patients_file}')
    logger.info(f'output path: {output_path}')
    logger.info(f'use_selenium_grid: {use_grid}')
    logger.info(f'earliest day to book appointments: {(datetime.now() + timedelta(day_offset)).date()}')

    Path('/opt/app-root/input/').mkdir(parents=True, exist_ok=True)
    run(patients_file, bot_url, use_grid, grid_url)
