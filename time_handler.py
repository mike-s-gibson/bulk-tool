import re
from datetime import *
from dateutil.relativedelta import *


class timeconverter(object):

    def get_placholder_conversion(self, prop, placeholder):

        placeholder = placeholder.lower()
        date_time_dict = {'date_only': {'props':['MoveDate', 'StartDate', 'InstallationDate', 'ScheduleStartDate'],
                                       'format': '%Y-%m-%d'},
                          'time_only': {'props':['ExecutionStartTime', 'BillingTime', 'ScheduleExecutionStartTime'],
                                       'format': '%H:%M:%S'},
                          't_date_time': {'props': ['ReadingsDateTime', 'AppointmentDateTime', 'InstallDateTime', 'RemovalDateTime'],
                                        'format': '%Y-%m-%dT%H:%M:%S'},
                          'tz_date_time': {'props': ['ExecutionDateTime', 'CurrentDateTime', 'StartDateTime',
                                                     'EndDateTime', 'SupplyEndDate', 'BillingPeriodStart'],
                                          'format': '%Y-%m-%dT%H:%M:%SZ'},

                          }

        placeholder_type = None
        fmat = None

        for key, value in date_time_dict.items():
            if prop in value['props']:
                placeholder_type = key
                fmat = value['format']

        if placeholder_type == 'date_only':
            calculated_timestamp = getattr(self, 'get_date_only', lambda: "Invalid Entry")(fmat, placeholder)
            return calculated_timestamp

        elif placeholder_type == 'time_only':
            calculated_timestamp = getattr(self, 'get_time_only', lambda: "Invalid Entry")(fmat, placeholder)
            return calculated_timestamp

        elif placeholder_type.endswith('date_time'):
            calculated_timestamp = getattr(self, 'get_date_and_time', lambda: "Invalid Entry")(fmat, placeholder)
            return calculated_timestamp

    def get_date_only(self, format, ph):
        pat = re.findall(r'(?=\*(.*?)\*)', ph)

        if pat[0] == 'now':
            date_output = datetime.utcnow().strftime(format)
        elif pat[0].startswith('tom'):
            date_output = (datetime.today() + timedelta(days=1)).strftime(format)
        elif pat[0].startswith('yest'):
            date_output = (datetime.today() - timedelta(days=1)).strftime(format)
        elif re.match(r'[0-9]{4}-(1[0-2]|0[1-9])-(3[01]|[12][0-9]|0[1-9])', pat[0]):
            date_output = pat[0]
        else:
            match_list = re.findall(r"now|[+-]|\d+d|\d+m|\d+y", pat[0])
            date_calc = self.do_date_conversions(match_list)

            if date_calc[1] == '+':
                date_output = (datetime.today() + relativedelta(days=date_calc[2], months=date_calc[3], years=date_calc[4])).strftime(format)
            else:
                date_output = (datetime.today() - relativedelta(days=date_calc[2], months=date_calc[3], years=date_calc[4])).strftime(format)

        return date_output

    def get_time_only(self, format, ph):
        pat = re.findall(r'(?=\*(.*?)\*)', ph)

        if pat[0] == 'now':
            time_output = datetime.utcnow().strftime(format)
        elif pat[0] == 'midnight':
            time_output = '00:00:00'
        elif re.match(r'(?:[01]\d|2[0-3]):(?:[0-5]\d):(?:[0-5]\d)', pat[0]):
            time_output = pat[0]
        else:
            match_list = re.findall(r"now|[+-]|\d+h|\d+m|\d+s", pat[0])
            time_calc = self.do_time_conversions(match_list)

            if time_calc[1] == '+':
                time_output = (datetime.utcnow() + timedelta(hours=time_calc[2], minutes=time_calc[3], seconds=time_calc[4])).strftime(format)
            else:
                time_output = (datetime.today() - timedelta(hours=time_calc[2], minutes=time_calc[3], seconds=time_calc[4])).strftime(format)

        return time_output

    def get_date_and_time(self, format, ph):
        pat = re.findall(r'(?=\*(.*?)\*)', ph)

        if pat[0] == 'now':
            date_output = datetime.utcnow().strftime('%Y-%m-%d')
        elif pat[0].startswith('tom'):
            date_output = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
        elif pat[0].startswith('yest'):
            date_output = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        elif re.match(r'[0-9]{4}-(1[0-2]|0[1-9])-(3[01]|[12][0-9]|0[1-9])', pat[0]):
            date_output = pat[0]
        else:
            date_match_list = re.findall(r"now|[+-]|\d+d|\d+m|\d+y", pat[0])
            date_calc = self.do_date_conversions(date_match_list)

            if date_calc[1] == '+':
                date_output = (datetime.today() + relativedelta(days=date_calc[2], months=date_calc[3], years=date_calc[4])).strftime('%Y-%m-%d')
            else:
                date_output = (datetime.today() - relativedelta(days=date_calc[2], months=date_calc[3], years=date_calc[4])).strftime('%Y-%m-%d')

        if pat[1] == 'now':
            time_output = datetime.utcnow().strftime('%H:%M:%S')
        elif pat[1] == 'midnight':
            time_output = '00:00:00'
        elif re.match(r'(?:[01]\d|2[0-3]):(?:[0-5]\d):(?:[0-5]\d)', pat[1]):
            time_output = pat[1]
        else:
            time_match_list = re.findall(r"now|[+-]|\d+h|\d+m|\d+s", pat[1])
            time_calc = self.do_time_conversions(time_match_list)

            if time_calc[1] == '+':
                time_output = (datetime.utcnow() + timedelta(hours=time_calc[2], minutes=time_calc[3], seconds=time_calc[4])).strftime('%H:%M:%S')
            else:
                time_output = (datetime.today() - timedelta(hours=time_calc[2], minutes=time_calc[3], seconds=time_calc[4])).strftime('%H:%M:%S')

        if format.upper().endswith('Z'):
            return f'{date_output}T{time_output}Z'
        else:
            return f'{date_output}T{time_output}'

    def do_date_conversions(self, l):

        now = False
        day = 0
        month = 0
        year = 0
        direction = '+'

        for item in l:
            if item == 'now':
                now = True
            elif item == '+':
                direction = '+'
            elif item == '-':
                direction = '-'
            elif item.endswith('d'):
                day = item.split('d')[0]
            elif item.endswith('m'):
                month = item.split('m')[0]
            elif item.endswith('y'):
                year = item.split('y')[0]

        return (now, direction, int(day), int(month), int(year))

    def do_time_conversions(self, l):

        now = False
        hours = 0
        minutes = 0
        seconds = 0
        direction = '+'

        for item in l:
            if item == 'now':
                now = True
            elif item == '+':
                direction = '+'
            elif item == '-':
                direction = '-'
            elif item.endswith('h'):
                hours = item.split('h')[0]
            elif item.endswith('m'):
                minutes = item.split('m')[0]
            elif item.endswith('s'):
                seconds = item.split('s')[0]

        return (now, direction, int(hours), int(minutes), int(seconds))