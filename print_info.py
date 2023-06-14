import sys
import traceback
import time

class Printer():

    def print_to_stream(self, message, stream):
        stream.write(message + '\n')
        stream.flush()

    def print_stdout(self, num, message):
        self.print_to_stream(f'[INFO]||{num}|| {message}', sys.stdout)

    def print_stderr(self, num, message):
        self.print_to_stream(f'[DEBUG]||{num}|| {message}', sys.stderr)

    def handle_traceback(self, e=None, d=None):
        error = traceback.format_exc().split('\n')
        self.print_stderr(1, '')
        self.print_stderr(1, '#' * 150)
        for item in error:
            self.print_stderr(1, f'\t\t{item}')
        if d:
            for k, v in d.items():
                self.print_stderr(1, f'\t\tNOTE >> {k}: {v}')

        self.print_stderr(1, '/' * 150)
        self.print_stderr(1, '')
        time.sleep(1)
