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

    def __init__(self, config, xml, service, webui, ss, tab, user, port, auth, sheet, failures_only, part_way, local):
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
        self.batch_ul = int(self.config['exe_actions']['batch_ul'])
        self.is_query = False
        self.ss = ss
        if sheet:
            self.sheet = sheet
            self.tab = tab
            try:
                self.hdrs = self.get_sheet_headers(self.sheet)
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
        chunk_size = len(rows) // num_threads
        lists = [rows[i:i + chunk_size] for i in range(1, len(rows), chunk_size)]

        return lists

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

    def bulk_send_function(self, rows, list_len, q):

        i = None
        make_edit = False

        multi_device_sheet = self.check_sheet_headers()

        col_letter = string.ascii_uppercase[self.hdrs.index("ATOM_result")]

        output_list = []

        if 'CHFLocation' in self.hdrs:
            make_edit = True

        try:
            for i, row in enumerate(rows):
                row_dict = dict(zip(self.hdrs, row))

                if self.part_way:
                    executed_row = row_dict.get('ATOM_result', None)

                    if executed_row:
                        output_list.append([executed_row])
                        continue

                elif self.failures:
                    executed_row = row_dict.get('ATOM_result', None)

                    if executed_row.endswith('ACCEPTED'):
                        batch_list.append([executed_row])
                        continue

                if multi_device_sheet:
                    if (len(row_dict['ImportMPxN']) > 10):
                        bd = eval(self.config['xml_jsons'][f'{self.xml.upper()}_ESME'])
                    else:
                        bd = eval(self.config['xml_jsons'][f'{self.xml.upper()}_GSME'])
                else:
                    bd = eval(self.config['xml_jsons'][self.xml.upper()])

                if make_edit:
                    row_dict = self.xicom_edits(row_dict)

                bd = self.ve.replace_placeholder_values(bd, row_dict)

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
                    if requests.exceptions.ConnectionError:
                        connected = self.disco_loop()
                        if connected:
                            outcome, req_id, failure_string = self.send_request(short_xml_name, xml_str)
                    else:
                        self.p.print_stderr(1, f'SEND FAILURE: bulk_send_function: {e}')
                        pass


                if outcome:
                    if self.local:
                        self.p.print_stdout(2, f'Sheet row {i + 2} {outcome}')
                    output_list.append([outcome])

                else:
                    try:
                        outcome, req_id, failure_string = self.get_new_xml_response(short_xml_name, xml_str)
                        if outcome:
                            if self.local:
                                self.p.print_stdout(2, f'Sheet row {i + 2} {outcome}')
                            output_list.append([outcome])
                        else:
                            if self.local:
                                self.p.print_stdout(2, f'Sheet row {i + 2} FAILED')
                            output_list.append(['FAILED'])
                    except Exception as e:
                        d = dict(short_xml_name=short_xml_name, xml_str=''.join(xml_str.split()))
                        self.p.handle_traceback(e, d)
                        pass
                if keyboard.is_pressed('q'):
                    self.p.print_stdout(2, "'Q' pressed, exiting script early")
                    break
        except SigTermException as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, f'Received signal interrupt, exiting thread!')
        except KeyboardInterrupt as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, f'Received interrupt, exiting thread!')
        except Exception as e:
            self.p.handle_traceback(e)
            self.p.print_stderr(1, F'LOOP ERROR: bulk_send_function: {e}')

        q.put((col_letter, i + (list_len+3), output_list))
        q.put(None)

    def process_split_rows(self, lists):

        processes = []

        list_len = 0

        for sublist in lists:
            processes.append(multiprocessing.Process(target=self.bulk_send_function, args=(sublist, list_len, self.q)))
            list_len = list_len + len(sublist)

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
                except Exception as e:
                    if not notified:
                        self.p.handle_traceback(e)
                        self.p.print_stderr(1, F'LOOP ERROR: process_split_rows: error: {e}')
                        notified = True
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

    def process_sequential_rows(self, rows):

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
        total_rows = len(rows[1:])
        loop_start_time = time.time()

        try:
            for i, row in enumerate(rows[1:]):

                xml_body = None
                row_dict = dict(zip(self.hdrs, row))

                if self.is_query:
                    if any(value is None for value in row_dict.values()):
                        fail_count += 1
                        batch_list.append(['FAILED'])
                        continue

                if self.part_way:
                    executed_row = row_dict.get('ATOM_result', None)

                    if executed_row:
                        if 'ACCEPTED' in executed_row:
                            pass_count += 1
                        else:
                            fail_count += 1
                        batch_list.append([executed_row])
                        continue

                elif self.failures:
                    executed_row = row_dict.get('ATOM_result', None)

                    if executed_row.endswith('ACCEPTED'):
                        pass_count += 1
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
                #outcome = 'XML ACCEPTED'

                try:
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
                        pass

                if outcome:
                    if 'ACCEPTED' in outcome:
                        pass_count += 1
                    else:
                        fail_count += 1

                    remaining = total_rows-i
                    batch_list.append([outcome])
                else:
                    try:
                        outcome, req_id, failure_string = self.get_new_xml_response(self.xml, xml_str)
                        if outcome:
                            if 'ACCEPTED' in outcome:
                                pass_count += 1
                            else:
                                fail_count += 1
                            batch_list.append([outcome])
                        else:
                            fail_count += 1
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

                    if not self.is_query:
                        continue_run = self.update_sheet_cols(col_letter, i+3, batch_list)
                        batch_list.clear()

                        if continue_run == 'FALSE':
                            self.p.print_stdout(2, "Spreadsheet stop flag has been selected, exiting run")
                            break
                    else:
                        batch_list.clear()

                    if remaining - 1 != 0:
                        self.p.print_stdout(2, f'PASS:{pass_count}  |FAIL:{fail_count}  |COMPLETED:{i + 1}  |REMAINING:{remaining-1}  |{estimated_date_time.strftime("%d/%m/%Y %H:%M")}')

                if self.config['exe_actions']['wait_throttle']:
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

        if self.is_query:
            self.p.print_std(2, f'Completed {i} rows')
        else:
            self.p.print_stdout(2, f'PASS:{pass_count}  |FAIL:{fail_count}  |COMPLETED:{i + 1}  |REMAINING:{remaining-1}  |{estimated_date_time.strftime("%d/%m/%Y %H:%M")}')
            self.update_sheet_cols(col_letter, i+3, batch_list)

    def check_if_query(self, ss):

        pattern = r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b.*?\b(FROM|INTO|SET)\b'

        try:
            if re.match(pattern, ss, re.IGNORECASE):
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

        self.check_columns_match()

        fast_processing = self.config['fast_processing']['split_data'].title()

        if fast_processing == 'True':
            num_threads = int(self.config['fast_processing']['num_threads'])
            lists = self.split_rows(rows, num_threads)
            self.p.print_stdout(2, f'Start Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            self.process_split_rows(lists)
            self.p.print_stdout(2, f'Finish Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
        else:
            self.p.print_stdout(2, f'Start Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            self.process_sequential_rows(rows)
            self.p.print_stdout(2, f'Finish Time = {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')

    def find_missing_hdr_items(self, xml_cols):
        missing_items = []
        for item in xml_cols:
            if item not in self.hdrs[1:-1]:
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
    xg = XmlGen(config, xml_name, service, webui, ss, tab, user, port, auth, sheet, only_failures, part_way, local)
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
