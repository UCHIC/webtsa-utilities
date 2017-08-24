"""

Contains constants and other common variables

"""
import datetime
import os
import sys


class Common(object):
    def __init__(self, args):
        self.DEBUG = True if '--debug' in args else False
        self.VERBOSE = True if '--verbose' in args or self.DEBUG else False

        """
        General constants and non-class variables
        """
        self.IS_WINDOWS = 'nt' in os.name
        self.PROJECT_DIR = str(os.path.dirname(os.path.realpath(__file__)))
        self.SETTINGS_FILE_NAME = self.PROJECT_DIR + '/settings.json'
        self.LOGFILE_DIR = '{}/logs/'.format(self.PROJECT_DIR)
        self.GUI_MODE = False

        print self.PROJECT_DIR
        print self.LOGFILE_DIR

        """
        Setup sys and other args
        """
        sys.path.append(os.path.dirname(self.PROJECT_DIR))
        InitializeDirectories([self.LOGFILE_DIR])
        # sys.stdout = CustomLogger(self.LOGFILE_DIR, single_file=self.DEBUG)
        CustomLogger(self.LOGFILE_DIR, debug_mode=self.DEBUG)

    def dump_settings(self):
        for key in self.__dict__:
            print '{:<35} {}'.format(str(key) + ':', self.__dict__[key])


def InitializeDirectories(directory_list):
    for dir_name in directory_list:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)


class CustomLogger(object):
    def __init__(self, logfile_dir, debug_mode=False):
        self.terminal = sys.stdout
        self.std_error = sys.stderr
        if debug_mode:
            file_name = '{}/Log_{}.txt'.format(logfile_dir, 'File')
        else:
            file_name = '{}/Log_{}.txt'.format(logfile_dir, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        self.LogFile = open(file_name, mode='w')

        sys.stdout = self
        if not debug_mode:
            sys.stderr = self

    def write(self, message):
        self.terminal.write(message)
        self.LogFile.write(message)


APP_SETTINGS = Common(sys.argv)
