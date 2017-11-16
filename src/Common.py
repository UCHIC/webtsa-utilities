"""

Contains constants and other common variables

"""
import datetime
import json
import os
import sys


class Common(object):
    def __init__(self, args):
        self.script_name = os.path.basename(args[0])[:-3]
        self.DEBUG = True if '--debug' in args else False
        self.VERBOSE = True if '--verbose' in args else False

        """
        General constants and non-class variables
        """
        self.IS_WINDOWS = 'nt' in os.name
        self.PROJECT_DIR = str(os.path.dirname(os.path.realpath(__file__)))
        self.LOGFILE_DIR = '{}/logs/'.format(self.PROJECT_DIR)

        """
        Setup sys and other args
        """
        sys.path.append(os.path.dirname(self.PROJECT_DIR))
        InitializeDirectories([self.LOGFILE_DIR])
        CustomLogger(self.LOGFILE_DIR, self.script_name, debug_mode=self.DEBUG)

        self.credentials = {}  # type: dict[Credentials]
        self.update_catalogs = False
        # self.update_catalogs = True
        self.update_influx = True
        # self.update_influx = False

        with open(self.PROJECT_DIR + '/settings.json') as settings_file:
            data = json.load(settings_file)
            for key in data.keys():
                self.credentials[key] = Credentials(key, data[key]['influx'], data[key]['tsa_catalog_source'],
                                                    data[key]['tsa_catalog_destination'])

    def dump_settings(self):
        for key in self.__dict__:
            print '{:<35} {}'.format(str(key) + ':', self.__dict__[key])


class Credentials(object):
    def __init__(self, name, influx, tsa_source, tsa_destination):
        self.name = name
        self.influx_credentials = influx
        self.tsa_catalog_source = tsa_source
        self.tsa_catalog_destination = tsa_destination

    def __str__(self):
        return '<Credential: {}>'.format(self.name)


def InitializeDirectories(directory_list):
    for dir_name in directory_list:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)


class CustomLogger(object):
    def __init__(self, logfile_dir, script_name, debug_mode=False):
        self.terminal = sys.stdout
        if debug_mode:
            file_name = '{}/Log_{}_{}.txt'.format(logfile_dir, script_name, 'File')
        else:
            file_name = '{}/Log_{}_{}.txt'.format(logfile_dir, script_name,
                                                  datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        self.LogFile = open(file_name, mode='w')

        sys.stdout = self
        if not debug_mode:
            sys.stderr = self

    def prefix_date(self, message):
        date_string = datetime.datetime.now().strftime('%H-%M-%S')
        return '{date}: {message}\n'.format(date=date_string, message=message)

    def write(self, message):
        if len(message) > 0 and not message.isspace():
            self.terminal.write(self.prefix_date(message))
            self.LogFile.write(self.prefix_date(message))
            self.LogFile.flush()
        else:
            return

    def flush(self):
        pass


APP_SETTINGS = Common(sys.argv)
