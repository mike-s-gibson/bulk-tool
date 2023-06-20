import configparser
import string
import os
import sys
from configparser import ConfigParser
import argparse
import inspect
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import getpass
import socket
import threading
import queue
import requests
from xml.dom import minidom
import signal
import requests.utils
import keyboard
import ssl
import re
import itertools
import serv_auth
from serv_auth import *
import smtplib
from email.message import EmailMessage
from print_info import *
from Crypto.Cipher import AES
import time_handler
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
#from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import xml_builder as xb
import multiprocessing
from multiprocessing import Process

cwd = os.getcwd()
SERVICE_ACCOUNT_FILE = 'qa-smart-automation-sa.json'

json_file_path = Path().joinpath(cwd, SERVICE_ACCOUNT_FILE).as_posix()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file_path

creds = service_account.Credentials.from_service_account_file\
    (filename=str(json_file_path),
     scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/cloud-platform",
             'https://www.googleapis.com/auth/spreadsheets'])

BS = 16
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')
unpad = lambda s : s[0:-ord(s[-1:])]


class SigTermException(Exception):
    pass


def sigtermhandler(signum, frame):
    raise SigTermException('sigterm')


class ServerComms:

    def __init__(self, config, webui, usr, port, local, p):
        self.config = config
        self.webui = webui
        self.local = local
        self.usr = usr
        self.machine_name = socket.gethostname()
        self.svr = self.config['server_details']
        self.host = str(self.svr['host'])
        self.port = str(port)
        self.p = p


    def test_connection(self, auth_string):
        if self.webui:
            hp = {'Username': 'AtomWebUI', 'MachineID': self.machine_name, 'Authorization': 'Basic ' + auth_string}
        else:
            hp = {'Username': self.usr, 'MachineID': self.machine_name, 'Authorization': 'Basic ' + auth_string}
        try:
            resp = requests.get(f'http://{self.host}:{self.port}/TestConnection', headers=hp)
            return resp
        except Exception as e:
            return 'error'

    def send_bol_request(self, xml_name, xml_str, auth_string):
        if self.webui:
            hp = {'Username': 'AtomWebUI', 'MachineID': self.machine_name, 'Requestor': self.usr, 'ScenarioID': xml_name,
              'Timeout': '0', 'Authorization': 'Basic ' + auth_string}
        else:
            hp = {'Username': self.usr, 'MachineID': self.machine_name, 'Requestor': self.usr, 'ScenarioID': xml_name,
                  'Timeout': '0', 'Authorization': 'Basic ' + auth_string}

        resp = None
        notified = False

        while resp is None:
            try:
                resp = requests.post(f'http://{self.host}:{self.port}/SendBolRequest', data=xml_str, headers=hp,
                                     timeout=int(self.config['requests']['timeout']))
                if self.local:
                    self.p.print_stdout(2, f'Request response: {resp}')
                    self.p.print_stdout(2, f'Request response content: {resp.content}')
                return resp
            except (ConnectionError, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout) as e:
                if not notified:
                    d = dict(header=hp, host=self.host, port=self.port, XML=xml_name, XML_String=xml_str)
                    self.p.handle_traceback(e, d)
                    self.p.print_stderr(1, f'CONNECTION ERROR: send_bol_request: {e}')
                    notified = True
                else:
                    self.p.print_stderr(1, f'CONNECTION ERROR: send_bol_request: {e} No response, trying again in 10 seconds')
                    sleep(10)
            except Exception as e:
                if not notified:
                    d = dict(header=hp, host=self.host, port=self.port, XML=xml_name, XML_String=xml_str)
                    self.p.handle_traceback(e, d)
                    self.p.print_stderr(1, f'SEND ERROR: send_bol_request: {e}')
                    notified = True
                else:
                    self.p.print_stderr(1, f'SEND ERROR: send_bol_request: {e} No response, trying again in 10 seconds')
                    sleep(10)


class ValueEditor:

    def __init__(self, config, p):
        super(ValueEditor, self).__init__()
        self.config = config
        self.p = p

    def append_hdr_items(self, hdr, short_name):

        exe_scenarios_list = self.config.get('preceding_scenario', 'exe_scenario').split(',')
        prev_scenario_list = self.config.get('preceding_scenario', 'prev_scenario').split(',')
        prev_scenario = dict(zip(exe_scenarios_list, prev_scenario_list))

        h = hdr.copy()
        h['BusinessScenarioID'] = short_name

        if short_name in prev_scenario.keys():
            h['PrecedingScenarioID'] = prev_scenario[short_name]

        return h

    def replace_placeholder_values(self, body, row_dict, key_match=None):

        if key_match:
            for k, v in body.items():
                if isinstance(v, dict):
                    self.replace_placeholder_values(v, row_dict, key_match)
                elif isinstance(v, list):
                    for index, item in enumerate(v):
                        if isinstance(item, dict):
                            self.replace_placeholder_values(item, row_dict, key_match)
                        else:
                            if item.replace("$", "") in row_dict.keys():
                                v[index] = row_dict[item.replace("$", "")]
                else:
                    if v.startswith("$"):
                        if k in row_dict.keys():
                            body[k] = row_dict[k]
                    elif str(v).startswith('*'):
                        time_converter = time_handler.timeconverter()
                        body[k] = time_converter.get_placholder_conversion(k, v)
                    else:
                        continue

        else:
            for k, v in body.items():
                if isinstance(v, dict):
                    self.replace_placeholder_values(v, row_dict, key_match)
                elif isinstance(v, list):
                    for index, item in enumerate(v):
                        if isinstance(item, dict):
                            self.replace_placeholder_values(item, row_dict, key_match)
                        else:
                            if item.replace("$", "") in row_dict.keys():
                                v[index] = row_dict[item.replace("$", "")]
                else:
                    if v.replace("$", "") in row_dict.keys():
                        ### ADDED TO REMOVE CAPS START
                        if row_dict[v.replace("$", "")] == 'FALSE' or row_dict[v.replace("$", "")] == 'TRUE':
                            body[k] = row_dict[v.replace("$", "")].lower()
                        ### ADDED TO REMOVE CAPS END
                        else:
                            body[k] = row_dict[v.replace("$", "")]
                        if body[k].startswith('*'):
                            time_converter = time_handler.timeconverter()
                            body[k] = time_converter.get_placholder_conversion(k, body[k])
                    elif v in row_dict.keys():
                        body[k] = row_dict[v]
                    elif str(v).startswith('*'):
                        time_converter = time_handler.timeconverter()
                        body[k] = time_converter.get_placholder_conversion(k, v)
                    else:
                        continue

        return body


class XmlGen:

    def __init__(self, config, xml, service, webui, ss, tab, user, port, auth, sheet, failures_only, part_way, local, multi):
        self.p = Printer()
        self.creds = creds
        self.service = service
        self.config = config
        self.xml = xml
        self.user = user
        self.serv_auth = CypherString(self.user)
        self.time_converter = time_handler.timeconverter()
        self.sc = ServerComms(self.config, webui, self.user, port, local, self.p)
        self.hdr = {'@xmlns:uts': 'http://www.utilisoft.co.uk/namespaces/dccwb/UtilisoftBOL_1.0',
                    'SourceID': '$$ReplaceSourceID$$', 'SourceCounter': '$$ReplaceSourceCounter$$'}
        self.ve = ValueEditor(self.config, self.p)
        self.q = multiprocessing.Queue()
        self.auth = auth
        self.failures = failures_only
        self.part_way = part_way
        self.local = local
        self.multi = multi
        self.batch_ul = int(self.config['exe_actions']['batch_ul'])
        self.is_query = False
        self.ss = ss
        if sheet:
            self.sheet = sheet
            self.tab = tab
            try:
                self.hdrs = self.get_sheet_headers(self.sheet)
                for item in self.hdrs:
                    if '.' in item:
                        self.p.print_stderr(1, f'SHEET ERROR: Old Sheet used')
                        self.p.print_stderr(2, f'You appear to be using an old sheet template, there is a column mismatch - please use the latest Google sheet template')
            except Exception as e:
                d = dict(Sheetname=self.ss)
                self.p.handle_traceback(e, d)
                self.p.print_stderr(1, f'DATA ERROR: XmlGen: {e}')
                self.p.print_stderr(2, f'Unable to read sheets data, check the tab name and/or the URL is correct')
                return
        else:
            self.sheet = None

    def connection_check(self, a):
        try:
            resp = self.sc.test_connection(a)

            if isinstance(resp, str):
                input("\nConnection Failure, check VPN is active and re-launch the script, press enter to exit...")
                sys.exit()

            if resp.status_code == 403:
                self.p.print_stderr(1, f'TEST CONNECTION ERROR: connection_check: User not allowed - Status Code: {str(resp.status_code)}')
                self.p.print_stderr(2, f"403 Authentication failure, check with ATOM team for access")
                time.sleep(4)
                sys.exit()

            elif resp.headers['StatusCode'] == '0':
                if self.local:
                    self.p.print_stout(2, 'Connection established!')
                return
            else:
                self.p.print_stderr(2, f"4Unable to communicate with server - Status Code: {resp.headers['StatusCode']} - "
                      f"check VPN connectivity")
                input("\n\nPress enter to exit...")
                sys.exit()

        except Exception as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, 'CONNECTION FAILURE: connection_check: Check VPN')
            self.p.print_stderr(2, 'CONNECTION FAILURE: Check VPN is active')
            time.sleep(8)
            return 'Connection Failed, check VPN is active'

    def get_cypher(self, ):
        k = datetime.utcnow().strftime("%Y%m%d")
        cipher = AESCipher(k)
        tag = datetime.utcnow().strftime("%d%m%Y")
        encrypted = cipher.encrypt(f'{self.user}.{tag}')
        return encrypted

    def get_sheet_headers(self, sheet):
        rows = sheet.values().get(spreadsheetId=self.ss, range=f'{self.tab}!A1:AA250000').execute()
        col_headers = rows.get('values', [])[0]
        return col_headers

    def get_sheet_cols(self, sheet):
        columns = sheet.values().get(spreadsheetId=self.ss, range=f'{self.tab}!A1:AA250000', majorDimension='COLUMNS').execute()
        return columns.get('values', [])

    def get_sheet_rows(self, sheet):
        rows = sheet.values().get(spreadsheetId=self.ss, range=f'{self.tab}!A1:AA250000').execute()
        return rows.get('values', [])

    def get_sheet_rows_and_columns(self, sheet):
        columns = sheet.values().get(spreadsheetId=self.ss, range=f'{self.tab}!A1:AA250000', majorDimension='COLUMNS').execute()
        rows = sheet.values().get(spreadsheetId=self.ss, range=f'{self.tab}!A1:AA250000').execute()
        return rows.get('values', []), columns.get('values', [])

    def split_rows(self, rows, num_threads):
        n = len(rows)
        sublist_size = n // num_threads  # Integer division
        remainder = n % num_threads
        sublists = []
        start_index = 0
        for i in range(num_threads):
            sublist_length = sublist_size + (1 if i < remainder else 0)
            sublists.append(rows[start_index:start_index + sublist_length])
            start_index += sublist_length
        return sublists


    def xicom_edits(self, row_dict):

        location_dict = {'A': 'Attic',
                         'B': 'Bedroom',
                         'C': 'Cellar/Basement',
                         'E': 'Indoors',
                         'F': 'Not Known',
                         'G': 'Garage/Greenhouse',
                         'H': 'Hall',
                         'I': 'Cupboard',
                         'K': 'Kitchen',
                         'L': 'Landing',
                         'M': 'Sub Station',
                         'O': 'Outbuilding/Barn',
                         'R': 'Ladder Required',
                         'S': 'Understairs',
                         'T': 'Toilet',
                         'U': 'Upstairs',
                         'V': 'Vestry',
                         'W': 'Under Window',
                         'X': 'Outside Box',
                         'Y': 'O/S box with restricted access',
                         'Z': 'Communal Cupboard'}

        if row_dict['CHFLocation'].upper() in location_dict.keys():
            row_dict['CHFLocation'] = location_dict[row_dict['CHFLocation'].upper()]
        else:
            row_dict['CHFLocation'] = 'Not Known'

        return row_dict

    def process_split_rows(self, lists):

        processes = []

        list_len = 0

        for sublist in lists:
            processes.append(multiprocessing.Process(target=self.process_sequential_rows, args=(sublist, list_len, self.q)))
            list_len += len(sublist)

        for process in processes:
            process.start()

        conn_flag = False
        continue_run = True
        notified = False

        try:
            while len(processes) > 0:
                time.sleep((0.5))
                try:
                    progress = self.q.get(timeout=1)
                    if progress is None:
                        processes.pop()
                    else:
                        continue_run = self.update_sheet_cols(progress[0], progress[1], progress[2])
                        if continue_run == 'FALSE':
                            self.p.print_stdout(2, "Spreadsheet stop flag has been selected, exiting run")
                            break
                except queue.Empty:
                    pass
                except Exception as e:
                    if not notified:
                        self.p.handle_traceback(e)
                        self.p.print_stderr(1, F'LOOP ERROR: process_split_rows: err: {e}')
                        notified = True
                        pass
                    else:
                        pass
        except SigTermException as e:
            self.p.handle_traceback(e)
            time.sleep(3)
            self.p.print_stderr(1, f'LOOP ERROR: process_split_rows: Received signal interrupt, exiting thread!')
        except KeyboardInterrupt as e:
            self.p.handle_traceback(e)
            time.sleep(3)
            self.p.print_stderr(1, f'LOOP ERROR: process_split_rows: Received interrupt, exiting thread!')
        except Exception as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, F'LOOP ERROR: process_split_rows: {e}')

        for process in processes:
            process.join()

    def check_sheet_headers(self):
        if 'ImportMPxN' in self.hdrs:
            return True
        else:
            return False

    def get_config_option(self, section, option):
        try:
            return self.config.get(section, option)
        except configparser.NoOptionError as e:
            d = dict(section=section, option=option)
            if '_dual' in str(e):
                pass
            else:
                self.p.handle_traceback(e, d)
                self.p.print_stderr(1, f'Config file Exception: {e}')
            return '{}'
        except Exception as e:
            d = dict(section=section, option=option)
            self.p.handle_traceback(e, d)
            self.p.print_stderr(1, f'Config file Exception: {e}')
            self.p.print_stderr(2, f'XML {self.xml} Config Exception: Please contact the ATOM team')

    def process_sequential_rows(self, rows, list_len=None, q=None):

        if not rows:
            self.p.print_stderr(2, f'No row data available, empty dataset returned, exiting script!')
            return

        i = None
        xml_body_template_esme_credit = None
        xml_body_template_esme_dual_credit = None
        xml_body_template_esme_prepay = None
        xml_body_template_esme_dual_prepay = None
        xml_body_template_gsme_credit = None
        xml_body_template_gsme_prepay = None
        xml_body_template_esme = None
        xml_body_template_gsme = None
        xml_body_template_esme_dual = None

        in_run_throttle = None
        continue_run = True
        col_letter = string.ascii_uppercase[self.hdrs.index("ATOM_result")]

        if self.xml == 'EICOM_AI_01':
            xml_body_template_esme_credit = self.get_config_option(self.xml.lower(), 'esme_c2c')
            xml_body_template_esme_dual_credit = self.get_config_option(self.xml.lower(), 'esme_dual_c2c')
            xml_body_template_esme_prepay = self.get_config_option(self.xml.lower(), 'esme_p2p')
            xml_body_template_esme_dual_prepay = self.get_config_option(self.xml.lower(), 'esme_dual_p2p')

        elif self.xml == 'GICOM_AI_01':
            xml_body_template_gsme_credit = self.get_config_option(self.xml.lower(), 'gsme_credit')
            xml_body_template_gsme_prepay = self.get_config_option(self.xml.lower(), 'gsme_prepay')

        else:
            xml_body_template_esme = self.get_config_option(self.xml.lower(), 'esme')
            xml_body_template_gsme = self.get_config_option(self.xml.lower(), 'gsme')
            xml_body_template_esme_dual = self.get_config_option(self.xml.lower(), 'esme_dual')

        batch_list = []
        pass_count = 0
        fail_count = 0
        remaining = 0
        estimated_date_time = datetime.now()
        total_rows = len(rows)
        loop_start_time = time.time()

        xml_headers_list = self.get_all_config_headers(self.xml.lower())
        try:
            for i, row in enumerate(rows):

                xml_body = None
                row_dict = dict(zip(self.hdrs, row))

                if self.is_query:
                    corrupted = False

                    response_status = row_dict.get('responseStatus', None)

                    if response_status:
                        if isinstance(response_status, dict):
                            process_success_flag = response_status.get('processSuccessFlag', None)
                            if process_success_flag and self.config['requests']['run_if_response_status_true'] == 'False':
                                self.p.print_stdout(2, f"Row {i} returning 'responseStatus.processSuccessFlag' as true, skipping row. DATA: {', '.join(f'{key} = {value}' for key, value in row_dict.items() if key in xml_headers_list)}")

                                pass_count+=1
                                if q:
                                    batch_list.append([f'XML PREVIOUSLY ACCEPTED - row {row_dict["TRUE"]}'])
                                else:
                                    batch_list.append([f'XML PREVIOUSLY ACCEPTED'])
                                continue

                    for key, value in row_dict.items():
                        if value is None or value == '' or value == 'null':
                            if key in xml_headers_list:
                                corrupted = True
                                fail_count += 1
                                if q:
                                    batch_list.append([f'FAILED - row {row_dict["TRUE"]}'])
                                else:
                                    batch_list.append([f'FAILED'])
                                break

                    if corrupted:
                        self.p.print_stderr(2, f"Missing critical row data for row {i}. DATA: {', '.join(f'{key} = {value}' for key, value in row_dict.items() if key in xml_headers_list)}")
                        continue


                if self.part_way:
                    executed_row = row_dict.get('ATOM_result', None)

                    if executed_row:
                        if 'ACCEPTED' in executed_row:
                            pass_count += 1
                        else:
                            fail_count += 1

                        if q:
                            batch_list.append([f'{executed_row} - row {row_dict["TRUE"]}'])
                        else:
                            batch_list.append([executed_row])
                        continue

                elif self.failures:
                    executed_row = row_dict.get('ATOM_result', None)

                    if executed_row.endswith('ACCEPTED'):
                        pass_count += 1
                        if q:
                            batch_list.append([f'{executed_row} - row {row_dict["TRUE"]}'])
                        else:
                            batch_list.append([executed_row])
                        continue

                if all(val is None or val == '' for val in row_dict.values()):
                    batch_list.append([''])
                    continue

                dual_flag_item = None

                if 'CHFLocation' in self.hdrs:
                    row_dict = self.xicom_edits(row_dict)

                if 'ImportMPxN' in self.hdrs:
                    if (len(row_dict['ImportMPxN']) > 10):
                        for item in self.hdrs:
                            if 'Price2' in item or 'RegisterReading2' in item:
                                dual_flag_item = item
                        if dual_flag_item:
                            if self.xml in self.config['xmls']['payment_xmls']:
                                payment_mode = row_dict['PaymentMode'].lower()
                                if len(row_dict[dual_flag_item]) <= 0:
                                    if payment_mode == 'c2c':
                                        xml_body = xml_body_template_esme_credit
                                    elif payment_mode == 'p2p':
                                        xml_body = xml_body_template_esme_prepay
                                else:
                                    if payment_mode == 'c2c':
                                        xml_body = xml_body_template_esme_dual_credit
                                    elif payment_mode == 'p2p':
                                        xml_body = xml_body_template_esme_dual_prepay

                            else:
                                if len(row_dict[dual_flag_item]) <= 0:
                                    xml_body = xml_body_template_esme
                                else:
                                    xml_body = xml_body_template_esme_dual
                        else:
                            xml_body = xml_body_template_esme
                    else:
                        if self.xml in self.config['xmls']['payment_xmls']:
                            row_dict = self.xicom_edits(row_dict)
                            payment_mode = row_dict['PaymentMode'].lower()
                            if payment_mode == 'c2c':
                                xml_body = xml_body_template_gsme_credit
                            elif payment_mode == 'p2p':
                                xml_body = xml_body_template_gsme_prepay
                        else:
                            xml_body = xml_body_template_gsme
                elif 'PaymentMode' in self.hdrs:
                    if row_dict['PaymentMode'].lower() == 'credit':
                        xml_body = xml_body_template_credit
                    else:
                        xml_body = xml_body_template_prepay
                elif xml_body_template_esme or xml_body_template_gsme:
                    if 'DeviceType' in self.hdrs:
                        if row_dict['DeviceType'].upper() == 'ESME':
                            xml_body = xml_body_template_esme
                        else:
                            xml_body = xml_body_template_gsme
                    else:
                        if len(self.hdrs) == 3:
                            xml_body = xml_body_template_esme
                        else:
                            self.p.print_stderr(1, f'LOOP EXCEPTION: process_sequential_rows: Missing XML JSON Template {self.xml}, No MPxN, No PaymentMode, No Device type')
                            self.p.print_stderr(2, f'XML Error ATOM_XML001: please contact the ATOM team for support')


                xml_header = self.ve.append_hdr_items(self.hdr, self.xml.split('_')[0])
                xml_body = self.ve.replace_placeholder_values(eval(xml_body), row_dict)

                xml_str = self.create_xml(xml_header, xml_body)

                if self.local:
                    self.p.print_stdout(2, f"{''.join(xml_str.split())}")

                outcome = None

                outcome = 'XML ACCEPTED'

                """try:
                    outcome, req_id, failure_string = self.send_request(self.xml, xml_str)                        
                except Exception as e:
                    d = dict(xml=self.xml, xml_str=''.join(xml_str.split()))
                    self.p.handle_traceback(e, d)
                    self.p.print_stderr(1, f'LOOP EXCEPTION: process_sequential_rows: {e}')
                    if socket.gaierror:
                        self.p.print_stderr(2, f'Network error, trying to reconnect')
                        connected = self.disco_loop()
                        if connected:
                            outcome, req_id, failure_string = self.send_request(self.xml, xml_str)
                    if requests.exceptions.ConnectionError:
                        self.p.print_stderr(2, f'BOL connection error, trying to reconnect')
                        connected = self.disco_loop()
                        if connected:
                            outcome, req_id, failure_string = self.send_request(self.xml, xml_str)
                    else:
                        self.p.print_stderr(1, f'SEND FAILURE: process_sequential_rows: {e}')
                        pass"""

                if outcome:
                    if 'ACCEPTED' in outcome:
                        pass_count += 1
                    else:
                        fail_count += 1

                    remaining = total_rows-i
                    if q:
                        batch_list.append([f'{outcome} - row {row_dict["TRUE"]}'])
                    else:
                        batch_list.append([outcome])
                else:
                    try:
                        outcome, req_id, failure_string = self.get_new_xml_response(self.xml, xml_str)
                        if outcome:
                            if 'ACCEPTED' in outcome:
                                pass_count += 1
                            else:
                                fail_count += 1

                            if q:
                                batch_list.append([f'{outcome} - row {row_dict["TRUE"]}'])
                            else:
                                batch_list.append([outcome])
                        else:
                            fail_count += 1
                            if q:
                                batch_list.append([f'FAILED - row {row_dict["TRUE"]}'])
                            else:
                                batch_list.append(['FAILED'])
                        remaining = total_rows - i
                    except Exception as e:
                        d = dict(xml=self.xml, xml_str=''.join(xml_str.split()))
                        self.p.handle_traceback(e, d)
                        pass


                if len(batch_list) >= self.batch_ul:

                    elapsed_time = time.time() - loop_start_time
                    estimated_completion_time_in_seconds = (elapsed_time / i) * (total_rows - i)
                    estimated_date_time = datetime.now() + timedelta(seconds=estimated_completion_time_in_seconds)

                    if not q:
                        if not self.input_queue.empty():
                            input_text = self.input_queue.get()
                            if input_text.startswith('SLEEP'):
                                try:
                                    in_run_throttle = float(input_text.split(':')[1])
                                    self.p.print_stdout(2,
                                                        f"Received SLEEP notification, each iteration will now sleep for - {in_run_throttle} seconds")
                                    self.p.print_stdout(2, f"To reset SLEEP notification, send SLEEP:0 to stdin")
                                except ValueError:
                                    self.p.print_stdout(2,
                                                        f"Received incompatible SLEEP notification - number must be an int or a float, run will continue")
                                    self.p.print_stdout(2, f"SLEEP notification example - SLEEP:0.2")
                            if input_text.startswith('STOP'):
                                self.p.print_stdout(2, "Received STOP run notifictation, exiting run")
                                break
                            if input_text.startswith('PAUSE'):
                                self.p.print_stdout(2, "Received PAUSE run notifictation, pausing run")
                                self.p.print_stdout(2,
                                                    "To un-pause the run send CONTINUE to stdin (may take up to 30 seconds to continue)")
                                while True:
                                    input_text = self.input_queue.get()
                                    if input_text.startswith('CONTINUE'):
                                        self.p.print_stdout(2, "Received CONTINUE run notifictation, continuing run")
                                        break
                                    else:
                                        time.sleep(30)

                        if not self.is_query:
                            continue_run = self.update_sheet_cols(col_letter, i+3, batch_list)
                            batch_list.clear()

                            if continue_run == 'FALSE':
                                self.p.print_stdout(2, "Spreadsheet stop flag has been selected, exiting run")
                                break
                        else:
                            batch_list.clear()

                    if q:
                        if continue_run == 'FALSE':
                            self.p.print_stdout(2, "Spreadsheet stop flag has been selected, exiting run")
                            break

                    elif remaining - 1 != 0:
                        self.p.print_stdout(2, f'PASS:{pass_count}  |FAIL:{fail_count}  |COMPLETED:{i + 1}  |REMAINING:{remaining-1}  |{estimated_date_time.strftime("%d/%m/%Y %H:%M")}')

                if in_run_throttle:
                    time.sleep(float(in_run_throttle))
                elif self.config['exe_actions']['wait_throttle']:
                    time.sleep(float(self.config['exe_actions']['wait_throttle']))

                if keyboard.is_pressed('q'):
                    self.p.print_stdout(2, "'Q' pressed, exiting script early")
                    break


        except SigTermException:
            self.p.handle_traceback(e)
            time.sleep(3)
            self.p.print_stderr(1, f'LOOP ERROR1: process_sequential_rows: Received signal interrupt, exiting thread!')

        except KeyboardInterrupt:
            self.p.handle_traceback(e)
            time.sleep(3)
            self.p.print_stderr(1, f'LOOP ERROR2: process_sequential_rows: Received interrupt, exiting thread!')

        except Exception as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, F'LOOP ERROR3: process_sequential_rows: {e}')

        if not q:
            if self.is_query:
                self.p.print_stdout(2, f'PASS:{pass_count}  |FAIL:{fail_count}  |COMPLETED:{i + 1}  |REMAINING:0  |{estimated_date_time.strftime("%d/%m/%Y %H:%M")}')
                self.p.print_stdout(2, f'Completed {i+1} rows')
            else:
                self.p.print_stdout(2, f'PASS:{pass_count}  |FAIL:{fail_count}  |COMPLETED:{i + 1}  |REMAINING:{remaining-1}  |{estimated_date_time.strftime("%d/%m/%Y %H:%M")}')
                self.update_sheet_cols(col_letter, i+3, batch_list)

        if q:
            self.p.print_stdout(2,f'PASS:{pass_count}  |FAIL:{fail_count}  |COMPLETED:{i + 1}  |REMAINING:{remaining-1}  |{estimated_date_time.strftime("%d/%m/%Y %H:%M")}', 'threaded')
            q.put((col_letter, i + (list_len + 3), batch_list))
            q.put(None)

    def check_if_query(self, ss):

        pattern = r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b.*?\b(FROM|INTO|SET)\b'

        try:
            if ss.startswith('with'):
                return True
            elif re.match(pattern, ss, re.IGNORECASE):
                return True
            else:
                return False
        except TypeError as e:
            self.p.handle_traceback(e)
            return False

    def get_query_rows(self, query):
        client = bigquery.Client(credentials=creds, project=creds.project_id,)

        query_job = client.query(query)  # API request

        self.hdrs = [schema.name for schema in query_job.result().schema]
        self.hdrs.append("ATOM_result")

        rows = [list(row.values()) for row in query_job.result()]

        return rows, self.hdrs

    def read_input(self):
        global msg
        msg = sys.stdin.readline().rstrip('\r\n')
        print(f'Do something with ({msg}) message received')

    def check_stdin(self, input_queue):
        """Thread function to check for stdin and report back input text."""
        while True:
            input_text = sys.stdin.readline().strip()
            input_queue.put(input_text)

    def bulk_xml_func(self, sheet):

        self.is_query = self.check_if_query(self.ss)

        if self.is_query:
            try:
                rows, self.hdrs = self.get_query_rows(self.ss)
            except Exception as e:
                d = dict(ss=self.ss)
                self.p.handle_traceback(e, d)
                self.p.print_stderr(1, f'DATA ERROR: bulk_xml_func: error collecting rows: {e}')
                self.p.print_stderr(2, f'Unable to view data, please check the sheet is correctly shared')
                return
        else:
            try:
                rows = self.get_sheet_rows(sheet)
            except Exception as e:
                d = dict(ss=self.ss)
                self.p.handle_traceback(e, d)
                self.p.print_stderr(1, f'DATA ERROR: bulk_xml_func: error collecting rows: {e}')
                self.p.print_stderr(2, f'Unable to view sheet data, please check the sheet is correctly shared, and the Google Sheets URL is correct')
                return

        if self.check_columns_match():

            if self.hdrs[0] == 'FALSE':
                self.p.print_stdout(2, "Spreadsheet run flag check box is unchecked, exiting run.")
                self.p.print_stdout(2, "To continue run make sure Spreadsheet run flag check box is checked, and re-submit run.")
            elif self.multi == 'True':
                num_threads = int(self.config['fast_processing']['num_threads'])
                lists = self.split_rows(rows[1:], num_threads)
                self.p.print_stdout(2, f'Start Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                self.process_split_rows(lists)
                self.p.print_stdout(2, f'Finish Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            else:
                self.input_queue = queue.Queue()
                # Create and start the thread to check stdin
                stdin_thread = threading.Thread(target=self.check_stdin, args=(self.input_queue,))
                stdin_thread.daemon = True  # Set the thread as a daemon, so it exits when the main thread ends
                stdin_thread.start()

                self.p.print_stdout(2, f'Start Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                self.process_sequential_rows(rows[1:])
                self.p.print_stdout(2, f'Finish Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
        else:
            return

    def find_missing_hdr_items(self, xml_cols):
        missing_items = []
        for item in xml_cols:
            if item not in self.hdrs:
                if item == 'ReplaceSourceCounter':
                    continue
                else:
                    missing_items.append(item)
        return missing_items

    def get_hdrs_from_xml(self, data, values_list):
        for k, v in data.items():
            if isinstance(v, dict):
                self.get_hdrs_from_xml(v, values_list)
            elif isinstance(v, list):
                for item in v:
                    self.get_hdrs_from_xml(item, values_list)
            elif isinstance(v, str) and v.startswith('$') and v.endswith('$'):
                value = v.strip('$')
                if value not in values_list:
                    values_list.append(value)
        return values_list

    def get_all_config_headers(self, section):
        values_list = []
        try:
            for key, value in self.config.items(section):
                values_list = self.get_hdrs_from_xml(eval(value), values_list)
        except Exception as e:
            d = dict(section=section)
            self.p.handle_traceback(e, d)
            self.p.print_stderr(1, 'Incorrect Config file section name')

        return values_list

    def check_columns_match(self):
        xml_headers_list = self.get_all_config_headers(self.xml.lower())
        missing_items = self.find_missing_hdr_items(xml_headers_list)
        if missing_items:
            self.p.print_stderr(2, f'missing columns {missing_items}')
            return False
        else:
            return True

    def exe_sheet(self):

        if self.xml in self.config['xmls']['unique_xmls']:
            self.xml_func = getattr(self, self.xml.lower())
            self.xml_func(self.sheet)
        else:
            self.bulk_xml_func(self.sheet)

        if self.config['exe_actions']['email_address'] != '':
            self.sendEmail(self.config['exe_actions']['email_address'])

        return

    def sendEmail(self, to, attachment_path=None):

        EMAIL_ADDRESS = 'python.training.ovo@gmail.com'
        EMAIL_PASSWORD = 'kokwirqpiptpehgl'

        msg = EmailMessage()
        msg['Subject'] = 'Smart Automation triggered email'
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = to

        preamble = f'<h1>Bulk Tool Run Update</h1>\n<p>RUN COMPLETE</p>\n<p>Please check Google Sheet</p>\n'

        send_msg = preamble

        msg.add_alternative(send_msg, subtype='html')

        if attachment_path:
            with open(attachment_path, 'rb') as pdf:
                msg.add_attachment(pdf.read(), maintype='application', subtype='octet-stream', filename=pdf.name)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            self.p.print_stdout(2, f'sending email to {to}')
            smtp.send_message(msg)

    def send_request(self, xml_name, xml_str):
        req_id = '000'
        failure_string = None

        resp = self.sc.send_bol_request(xml_name, xml_str, self.auth)

        if str(resp.status_code).startswith('403'):
            self.auth = self.serv_auth.auth_string()
            resp = self.sc.send_bol_request(xml_name, xml_str, self.auth)


        if resp.headers['StatusCode'] == '0':
            req_id = re.findall('[0-9]+', resp.content.decode('utf-8').replace("\"", ""))[0]
            outcome = 'XML ACCEPTED'
        else:
            outcome = 'FAILED'
            failure_string = resp.headers['StatusString']
            self.p.print_stdout(2, f'Send Request failed: {failure_string}')

        return (outcome, req_id, failure_string)

    def update_sheet_cols(self, col_letter, count, progress):
        notified = False
        while True:
            try:
                resource = {"majorDimension": "ROWS", "values": progress}
                response = self.service.spreadsheets().values().update(spreadsheetId=self.ss, range=f"{self.tab}!{col_letter}{str(count-len(progress))}:{col_letter}{str(count)}", body=resource,valueInputOption="USER_ENTERED").execute()
                break
            except Exception as e:
                if not notified:
                    self.p.handle_traceback(e)
                    notified = True
                if socket.gaierror:
                    self.p.print_stderr(2, f'Network error, trying to reconnect')
                    time.sleep(10)
                    connected = self.disco_loop()
                    if connected:
                        continue
                elif 'TimeoutError: The read operation timed out' in str(e):
                    self.p.print_stderr(2, 'Google Sheets connection error, Retrying in 10 seconds')
                    time.sleep(10)
                    continue
                elif 'HttpError 429' in str(e):
                    self.p.print_stderr(1, 'GOOGLE SHEETS ERROR: update_sheet_cols: Maximum Google Sheets write limit exceeded, upping upload block rate by 5, and retrying in 30 seconds')
                    self.p.print_stderr(2, 'Maximum Google Sheets write limit exceeded, retrying in 30 seconds')
                    self.batch_ul = self.batch_ul + 5
                    time.sleep(30)
                    continue
                elif httplib2.error.ServerNotFoundError:
                    self.p.print_stderr(2, f'Network error, trying to reconnect')
                    time.sleep(10)
                    connected = self.disco_loop()
                    if connected:
                        continue
                else:
                    self.p.print_stderr(1, f'MAJOR UPDATE SHEETS ERROR: update_sheet_cols: {e}')

        run_dir = Path.cwd()



        if len([file for file in os.listdir(run_dir) if file.startswith(self.ss)]) >= 1:
            for fname in os.listdir(run_dir):
                if fname.startswith(self.ss):
                    os.remove(os.path.join(run_dir, fname))
            return 'FALSE'
        else:
            run_headers = self.get_sheet_headers(self.sheet)
            return run_headers[0]


    def disco_loop(self):
        while True:
            resp = self.sc.test_connection(self.auth)
            if resp == 'error':
                time.sleep(2)
            elif resp.status_code == 200:
                return
            else:
                time.sleep(2)

    def xjoin_in_01(self, sheet):

        cols = self.get_sheet_cols(sheet)

        d = {}

        for col in cols:
            list_header = col[0]
            col = col[1:]
            d[list_header] = col

        processes = []

        delay = int(self.config['parallel_xmls']['delay_in_secs_between_threads'])

        p2e_col_letter = string.ascii_uppercase[self.hdrs.index("PPMID-ESME Status")]
        e2p_col_letter = string.ascii_uppercase[self.hdrs.index("ESME-PPMID Status")]
        gpf2p_col_letter = string.ascii_uppercase[self.hdrs.index("GPF-PPMID Status")]
        p2g_col_letter = string.ascii_uppercase[self.hdrs.index("PPMID-GSME Status")]
        g2p_col_letter = string.ascii_uppercase[self.hdrs.index("GSME-PPMID Status")]

        signal.signal(signal.SIGTERM, sigtermhandler)

        processes.append(multiprocessing.Process(target=self.xjoin_function, args=(d, self.q, (0, 'ImportMPAN', 'PPMIDDeviceID', 'ESMEDeviceID', 'PPMID-ESME Status', 'xjoin_ppmid_2_esme', p2e_col_letter))))
        processes.append(multiprocessing.Process(target=self.xjoin_function, args=(d, self.q, (delay, 'ImportMPAN', 'ESMEDeviceID', 'PPMIDDeviceID', 'ESME-PPMID Status', 'xjoin_esme_2_ppmid', e2p_col_letter))))
        processes.append(multiprocessing.Process(target=self.xjoin_function, args=(d, self.q, (delay * 2, 'ImportMPRN', 'GPFDeviceID', 'PPMIDDeviceID', 'GPF-PPMID Status', 'xjoin_gpf_2_ppmid', gpf2p_col_letter))))
        processes.append(multiprocessing.Process(target=self.xjoin_function, args=(d, self.q, (delay * 3, 'ImportMPRN', 'PPMIDDeviceID', 'GSMEDeviceID', 'PPMID-GSME Status', 'xjoin_ppmid_2_gsme', p2g_col_letter))))
        processes.append(multiprocessing.Process(target=self.xjoin_function, args=(d, self.q, (delay * 4, 'ImportMPRN', 'GSMEDeviceID', 'PPMIDDeviceID', 'GSME-PPMID Status', 'xjoin_gsme_2_ppmid', g2p_col_letter))))

        for process in processes:
            process.start()

        conn_flag = False
        continue_run = True
        notified = False

        try:
            while len(processes) > 0:
                time.sleep((0.5))

                try:
                    progress = self.q.get(timeout=1)
                    if progress is None:
                        processes.pop()
                        conn_flag = False
                    else:
                        continue_run = self.update_sheet_cols(progress[0], progress[1], progress[2])
                        if continue_run == 'FALSE':
                            self.p.print_stdout(2, "Spreadsheet stop flag has been selected, exiting run")
                            break

                        conn_flag = False
                except Exception as e:
                    if not notified:
                        self.p.handle_traceback(e)
                        self.p.print_stderr(1, f'CONNECTION ERROR: xjoin_in_01: {e}')
                        notified = True

                    if socket.gaierror:
                        if not conn_flag:
                            self.p.print_stderr(2, 'connecting...')
                            conn_flag = True

                    if requests.exceptions.ConnectionError:
                        pass
                        if not conn_flag:
                            self.p.print_stderr(2, 'connecting...')
                            conn_flag = True

                    pass
        except KeyboardInterrupt as e:
            self.p.handle_traceback(e)
            time.sleep(3)
            self.p.print_stderr(1, f'Exiting main thread')
        except SigTermException as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, f'Exiting main thread')
        except Exception as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, f'CONNECTION ERROR2: xjoin_in_01: {e}')


        for process in processes:
            process.join()

        finish_time = datetime.now()
        self.p.print_stdout(2, f'Finish Time = {finish_time.strftime("%d/%m/%Y %H:%M:%S")}')

        return

    def create_xml(self, hd, bd):

        xml = xb.BuildXml(hd, bd)

        root = xml.create_xml()

        xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="   ")
        xml_str = re.sub(r"&amp;", r"&", xml_str)

        return xml_str

    def get_new_xml_response(self, short_xml_name, xml_str):
        try:
            outcome, req_id, failure_string = self.send_request(short_xml_name, xml_str)
            return outcome, req_id, failure_string
        except Exception as e:
            d = dict(short_xml_name=short_xml_name, xml_str=''.join(xml_str.split()))
            self.p.handle_traceback(e, d)
            if socket.gaierror:
                connected = self.disco_loop()
                if connected:
                    outcome, req_id, failure_string = self.send_request(short_xml_name, xml_str)
                    return outcome, req_id, failure_string
            if requests.exceptions.ConnectionError:
                connected = self.disco_loop()
                if connected:
                    outcome, req_id, failure_string = self.send_request(short_xml_name, xml_str)
                    return outcome, req_id, failure_string
            else:
                self.p.print_stderr(1, f'SEND FAILURE: get_new_xml_response: {e}')
                pass


    def xjoin_function(self, d, q, params):

        sleep_count = params[0]

        try:
            while sleep_count > 0:
                time.sleep(1)
                sleep_count -= 1
        except SigTermException as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, f'PID: {os.getpid()} received signal interrupt, exiting thread!')
        except KeyboardInterrupt as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, f'PID: {os.getpid()} received interrupt, exiting thread!')
        except Exception as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, f'FUNCTION ERROR: xjoin_function: {e}')

        if sleep_count <= 0:
            device_ids = d[params[2]]
            other_device_ids = d[params[3]]
            mpxns = d[params[1]]
            res = d[params[4]]
            batch = self.batch_ul

            total_exe_time = 0
            num_completed_iterations = 0
            running_average = 0

            try:
                assert len(device_ids) == len(other_device_ids) == len(mpxns)
            except AssertionError as e:
                self.p.handle_traceback(e)
                self.p.print_stderr(1, f'FUNCTION ERROR: xjoin_function: Columns are not the same lengths: {e}')
            except Exception as e:
                self.p.handle_traceback(e)
                self.p.print_stderr(1, f'FUNCTION ERROR2: xjoin_function: {e}')

            status_list = []
            idx = None

            try:
                for idx, (dev, other_dev, dev_mpxn, dev_res) in enumerate(itertools.zip_longest(device_ids, other_device_ids, mpxns, res)):
                    start_time = time.time()
                    row_dict = {'DeviceID': dev, 'OtherDeviceID': other_dev, 'ImportMPAN': dev_mpxn, 'ImportMPRN': dev_mpxn, params[4]: dev_res}

                    if all(val is None or val =='' for val in row_dict.values()):
                        continue

                    if row_dict['DeviceID'] is None or row_dict['DeviceID'] == '':
                        time.sleep(running_average)
                        executed_row = row_dict.get(params[4], None)
                        if executed_row:
                            status_list.append([executed_row])
                            continue
                        else:
                            status_list.append(['Missing Device'])
                            continue

                    elif row_dict['OtherDeviceID'] is None or row_dict['OtherDeviceID'] == '':
                        time.sleep(running_average)
                        executed_row = row_dict.get(params[4], None)
                        if executed_row:
                            status_list.append([executed_row])
                            continue
                        else:
                            status_list.append(['Missing Device'])
                            continue

                    if self.part_way:
                        executed_row = row_dict.get(params[4], None)
                        if executed_row:
                            status_list.append([executed_row])
                            continue

                    elif self.failures:
                        executed_row = row_dict.get(params[4], None)

                        if executed_row.endswith('ACCEPTED'):
                            status_list.append([executed_row])
                            continue



                    bd = eval(self.config['xml_jsons'][params[5].upper()])

                    bd = self.ve.replace_placeholder_values(bd, row_dict, True)

                    short_xml_name = list(bd.keys())[0]

                    hd = self.ve.append_hdr_items(self.hdr, short_xml_name.split('_')[0])

                    xml_str = self.create_xml(hd, bd)

                    if self.local:
                        self.p.print_stdout(2, xml_str)

                    outcome = None

                    try:
                        outcome, req_id, failure_string = self.send_request(short_xml_name, xml_str)
                    except Exception as e:
                        d = dict(short_xml_name=short_xml_name, xml_str=''.join(xml_str.split()))
                        self.p.handle_traceback(e, d)
                        if socket.gaierror:
                            connected = self.disco_loop()
                            if connected:
                                outcome, req_id, failure_string = self.send_request(short_xml_name, xml_str)
                        elif requests.exceptions.ConnectionError:
                            connected = self.disco_loop()
                            if connected:
                                outcome, req_id, failure_string = self.send_request(short_xml_name, xml_str)
                        else:
                            self.p.print_stderr(1, f'SEND FAILURE: xjoin_function: {e}')
                            pass

                    #outcome = 'XML ACCEPTED'

                    if len(status_list) >= batch:
                        q.put((params[6], idx+2, status_list))

                        status_list = []

                    if outcome:
                        if self.local:
                            self.p.print_stdout(2, f'Sheet row {idx+2} {outcome}')
                        status_list.append([outcome])

                    else:
                        status_list.append(['FAILED'])

                    end_time = time.time()
                    execution_time = end_time - start_time

                    total_exe_time += execution_time
                    num_completed_iterations += 1
                    running_average = total_exe_time / num_completed_iterations

            except SigTermException as e:
                self.p.handle_traceback(e)
                self.p.print_stderr(1, f'PID: {os.getpid()} received signal interrupt, exiting thread!')
            except KeyboardInterrupt as e:
                self.p.handle_traceback(e)
                self.p.print_stderr(1, f'PID: {os.getpid()} received interrupt, exiting thread!')
            except Exception as e:
                self.p.handle_traceback(e)
                self.p.print_stderr(1, f'LOOP ERROR: xjoin_function: {e}')


            q.put((params[6], idx+3, status_list))
            q.put(None)
            return status_list

    def set_placeholders(self, key, val, d):

        for k, v in d.items():
            if isinstance(v, dict):
                self.set_placeholders(key, val, v)
            elif v == key:
                d.update({k: val})
        return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-webui', '--webui', default=True)
    parser.add_argument('-user', '--username')
    parser.add_argument('-xml', '--xml')
    parser.add_argument('-ss', '--spreadsheet')
    parser.add_argument('-tab', '--tab', default=None)
    parser.add_argument('-port', '--port')
    parser.add_argument('-fail', '--only_failures', default=False)
    parser.add_argument('-cont', '--cont', default=False)
    parser.add_argument('-local', '--local', default=False)
    parser.add_argument('-multi', '--multi', default=False)

    args = parser.parse_args()

    config = ConfigParser()
    run_dir = Path.cwd()
    config.read(f'{run_dir}/conf.ini')

    webui = args.webui
    user = args.username
    xml_name = args.xml
    ss = args.spreadsheet
    tab = args.tab
    only_failures = args.only_failures
    part_way = args.cont
    local = args.local
    port = args.port
    multi = args.multi

    try:
        service = build('sheets', 'v4', credentials=creds)
    except:
        DISCOVERY_SERVICE_URL = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
        service = build('sheets', 'v4', credentials=creds, discoveryServiceUrl=DISCOVERY_SERVICE_URL)


    cypher = serv_auth.CypherString(user)
    auth = cypher.auth_string()
    sheet = None

    try:
        sheetInfo = service.spreadsheets().get(spreadsheetId=ss).execute()
        sheet = service.spreadsheets()
    except ssl.SSLEOFError:
        print('Error connecting to sheet')
    except:
        pass

    stdoutOrigin = None

    if local:
        print('Starting XML Generator')
        #stdoutOrigin = sys.stdout
        #sys.stdout = open("C:/logs/log.txt", "w")
    xg = XmlGen(config, xml_name, service, webui, ss, tab, user, port, auth, sheet, only_failures, part_way, local, multi)
    #try:
    xg.exe_sheet()
    #except Exception as e:
    #    print(f'CATCH ALL ERROR, {e}')


    #if local:
    #    sys.stdout.close()
    #    sys.stdout = stdoutOrigin
    # select PPMID_DeviceID,ImportMPAN,CHF_DeviceID from energy-services-prod.s_meter_reporting.V_IHD_AUTOMATION
    # python xml_generator.py -user Mike.Gibson32 -xml EICOM_AI_01 -ss 1sITjGOVE6_b5lOlldk44bdrjsD74-2DXl6cEr8Htj-8 -tab EICOM_AI_01_template -port 8003
    # python xml_generator.py -user Mike.Gibson32 -xml XRIDY_IN_01 -ss 1sITjGOVE6_b5lOlldk44bdrjsD74-2DXl6cEr8Htj-8 -tab XRIDY_IN_01_template -port 8003
    # python xml_generator.py -user Mike.Gibson32 -xml XIPMD_IN_01 -ss "select PPMID_DeviceID as PPMIDDeviceID,ImportMPAN as ImportMPxN,CHF_DeviceID as CHFDeviceID from energy-services-prod.s_meter_reporting.V_IHD_AUTOMATION" -port 8003
    # SELECT distinct A.SERVICE_POINT as ImportMPxN,FLOW_MSN as MSN FROM `energy-services-prod.s_meter_reporting.T_OVO_NONCOMM_MORE_THAN_WEEK` as A inner join `boost-operations.Payers.tbl_Non_Payer_Services_Reasons` as B on A.SERVICE_POINT = B.Meter_Point_No where TO_DATE = '2023-05-21' and FLOW_MSN is NOT null
    # python xml_generator.py -user Mike.Gibson32 -xml XIPMD_IN_01 -ss "SELECT distinct A.SERVICE_POINT as ImportMPxN,FLOW_MSN as MSN FROM `energy-services-prod.s_meter_reporting.T_OVO_NONCOMM_MORE_THAN_WEEK` as A inner join `boost-operations.Payers.tbl_Non_Payer_Services_Reasons` as B on A.SERVICE_POINT = B.Meter_Point_No where TO_DATE = '2023-05-21' and FLOW_MSN is NOT null" -port 8003

    # with NetlynkData AS(SELECT Shipment_Date,PPMID_DeviceID AS PPMIDDeviceID,ImportMPAN AS ImportMPxN,CHF_DeviceID AS CHFDeviceID,PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT(Shipment_Date, r'(\d{2}/\d{2}/\d{4})')) AS parsed_date FROM `energy-services-prod.s_meter_reporting.V_IHD_AUTOMATION` WHERE Order_Type = 'GEO2_PAYM'),responsedata AS (SELECT * FROM `data-engineering-prod.landing_bws_secure.bws_install_ppmid_response_v1`,  UNNEST (meters))select NetlynkData.*,responsedata.* from NetlynkData LEFT JOIN responsedata ON responsedata.data.InstallPPMIDResponseData.deviceId =  NetlynkData.PPMIDDeviceIDWHERE DATE(parsed_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    # python xml_generator.py -user Mike.Gibson32 -xml XIPMD_IN_01 -ss "with NetlynkData AS(SELECT Shipment_Date,PPMID_DeviceID AS PPMIDDeviceID,ImportMPAN AS ImportMPxN,CHF_DeviceID AS CHFDeviceID,PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT(Shipment_Date, r'(\d{2}/\d{2}/\d{4})')) AS parsed_date FROM `energy-services-prod.s_meter_reporting.V_IHD_AUTOMATION` WHERE Order_Type = 'GEO2_PAYM' AND Order_Reference IN('OVOEC2 - 59496','OVOEC2 - 64825','OVOEC2 - 64826','OVOEC2 - 64827','OVOEC2 - 64828','OVOEC2 - 64829','OVOEC2 - 64830','OVOEC2 - 64831','OVOEC2 - 64832','OVOEC2 - 64833','OVOEC2 - 64834','OVOEC2 - 64835','OVOEC2 - 64836','OVOEC2 - 64837','OVOEC2 - 64838','OVOEC2 - 64839','OVOEC2 - 64840','OVOEC2 - 64841','OVOEC2 - 64842','OVOEC2 - 64843','OVOEC2 - 64844','OVOEC2 - 64845','OVOEC2 - 64846','OVOEC2 - 64847','OVOEC2 - 64848','OVOEC2 - 64849','OVOEC2 - 64850','OVOEC2 - 64851','OVOEC2 - 64852','OVOEC2 - 64853','OVOEC2 - 64854','OVOEC2 - 64855','OVOEC2 - 64856','OVOEC2 - 64857','OVOEC2 - 64858','OVOEC2 - 64859','OVOEC2 - 64860','OVOEC2 - 64861','OVOEC2 - 64862','OVOEC2 - 64863','OVOEC2 - 64864','OVOEC2 - 64865','OVOEC2 - 64866','OVOEC2 - 64867','OVOEC2 - 64868','OVOEC2 - 64869','OVOEC2 - 64870','OVOEC2 - 64871','OVOEC2 - 64872','OVOEC2 - 64873','OVOEC2 - 64874','OVOEC2 - 64875','OVOEC2 - 64876','OVOEC2 - 64877','OVOEC2 - 64878','OVOEC2 - 64879','OVOEC2 - 64880','OVOEC2 - 64881','OVOEC2 - 64882','OVOEC2 - 64883','OVOEC2 - 64884','OVOEC2 - 64885','OVOEC2 - 64886','OVOEC2 - 64887','OVOEC2 - 64888','OVOEC2 - 64889','OVOEC2 - 64890','OVOEC2 - 64891','OVOEC2 - 64892','OVOEC2 - 64893','OVOEC2 - 64894','OVOEC2 - 64895','OVOEC2 - 64896','OVOEC2 - 64897','OVOEC2 - 64898','OVOEC2 - 64899','OVOEC2 - 64900','OVOEC2 - 64901','OVOEC2 - 64902','OVOEC2 - 64903','OVOEC2 - 64904','OVOEC2 - 64905','OVOEC2 - 64906','OVOEC2 - 64907','OVOEC2 - 64908','OVOEC2 - 64909','OVOEC2 - 64910','OVOEC2 - 64911','OVOEC2 - 64912','OVOEC2 - 64913','OVOEC2 - 64914','OVOEC2 - 64915','OVOEC2 - 64916','OVOEC2 - 64917','OVOEC2 - 64918','OVOEC2 - 64919','OVOEC2 - 64920','OVOEC2 - 64921','OVOEC2 - 64922','OVOEC2 - 64923','OVOEC2 - 64924')),responsedata AS (SELECT * FROM `data-engineering-prod.landing_bws_secure.bws_install_ppmid_response_v1`,  UNNEST (meters) where metadata.traceToken IS NULL)select NetlynkData.*,responsedata.* from NetlynkData LEFT JOIN responsedata ON responsedata.data.InstallPPMIDResponseData.deviceId =  NetlynkData.PPMIDDeviceIDWHERE DATE(parsed_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)" -port 8003
    #