"""Checks the repository for updates."""

import os
import sys
import urllib
import imp
import wx
from hashlib import md5
from inspect import getsourcelines
from threading import Thread
from retriever import REPOSITORY, VERSION, MASTER_BRANCH
from retriever.lib.models import file_exists
from retriever.app.splash import Splash

global abort, executable_name
abort = False
executable_name = "retriever"


def download_from_repository(filepath, newpath, repo=REPOSITORY):
    """Downloads the latest version of a file from the repository."""
    try:
        filename = filepath.split('/')[-1]
        latest = urllib.urlopen(repo + filepath, 'rb')
        file_size = latest.info()['Content-Length']
        new_file = open(os.path.join(os.getcwd(), newpath), 'wb')
        total_dl = 0
        while not abort:
            data = latest.read(1024)
            total_dl += len(data)
            if file_size > 102400:
                print str(int(total_dl / float(file_size) * 100)) + "-" + filename
            if len(data) == 0:
                break
            new_file.write(data)
        new_file.close()
        latest.close()
    except:
        pass


def more_recent(latest, current):
    """Given two version number strings, returns True if the first is more recent."""
    if current == "master":
        return False
    latest_parts = latest.split('.')
    current_parts = current.split('.')
    for n in range(len(latest_parts)):
        l = latest_parts[n]
        if len(current_parts) < (n + 1):
            return (l != "rc")
        c = current_parts[n]
        if l > c:
            return True
        elif c > l:
            return False
    return (len(current_parts) > (n + 1) and current_parts[n + 1] == "rc")


def check_for_updates():
    """Check for updates to scripts and executable."""
    app = wx.PySimpleApp()
    splash = Splash()
    #splash.Show()
    splash.SetText("\tLoading...")
    
    class update_progress:
        def __init__(self, parent):
            self.parent = parent
        def write(self, s):
            if s != "\n":
                try:
                    self.parent.SetText('\t' + s)
                except:
                    pass
                
    sys.stdout = update_progress(splash)
    
    init = InitThread()
    init.run()
    
    splash.Hide()
    sys.stdout = sys.__stdout__
    
    
class InitThread(Thread):
    """This thread performs all of the necessary updates while the splash screen
    is shown.

    1. Check master/version.txt to get the latest version (Windows only).
    2. Prompt for update if necessary (Windows only).
    3. Download latest versions of scripts from current branch."""
    def run(self):
        try:
            running_from = os.path.basename(sys.argv[0])
            
            if os.path.isfile('dbtk_old.exe') and running_from != 'dbtk_old.exe':
                try:
                    os.remove('dbtk_old.exe')
                except:
                    pass

            if running_from[-4:] == ".exe":
                # Windows: open master branch version file to find out most recent executable version            
                try:
                    version_file = urllib.urlopen(MASTER_BRANCH + "version.txt")
                except IOError:
                    print "Couldn't open version.txt from repository"
                    return
                    
                latest = version_file.readline().strip('\n')
                    
                if more_recent(latest, VERSION):
                        msg = "You're running version " + VERSION + "."
                        msg += '\n\n'
                        msg += "Version " + latest + " is available. Do you want to upgrade?"
                        choice = wx.MessageDialog(None, msg, "Update", wx.YES_NO)
                        if choice.ShowModal() == wx.ID_YES:
                            print "Updating to latest version. Please wait..."
                            try:
                                if not "_old" in running_from:
                                    os.rename(running_from,
                                              '.'.join(running_from.split('.')[:-1])
                                              + "_old." + running_from.split('.')[-1])
                            except:
                                pass
                                
                            download_from_repository("windows/" + executable_name + ".exe", 
                                                     executable_name + ".exe")

                            sys.stdout = sys.__stdout__

                            wx.MessageBox("Update complete. The program will now restart.")

                            os.execv(executable_name + ".exe", sys.argv)
                            
                            sys.exit()

                version_file.close()

            # open version.txt for current release branch and get script versions
            version_file = urllib.urlopen(REPOSITORY + "version.txt")
            version_file.readline()
            scripts = []
            for line in version_file:
                scripts.append(line.strip('\n').split(','))
            
            # get script files
            if not os.path.isdir("scripts"):
                os.mkdir("scripts")
            for script in scripts:
                script_name = script[0]
                if len(script) > 1:
                    script_version = script[1]
                else:
                    script_version = None

                if not file_exists(os.path.join("scripts", script_name)):
                    # File doesn't exist: download it
                    print "Downloading script: " + script_name
                    download_from_repository("scripts/" + script_name,
                                             "scripts/" + script_name)
                elif script_version:
                    # File exists: import and check MD5 sum
                    need_to_download = False
                    try:
                        file, pathname, desc = imp.find_module(''.join(script_name.split('.')[:-1]), 
                                                               ["scripts"])
                        new_module = imp.load_module(script_name, file, pathname, desc)
                        m = md5()
                        m.update(''.join(getsourcelines(new_module)[0]))
                        m = m.hexdigest()
                        need_to_download = script_version != m
                    except:            
                        pass
                                
                    if need_to_download:
                        try:
                            os.remove(os.path.join("scripts", script_name))
                            download_from_repository("scripts/" + script_name,
                                                     "scripts/" + script_name)
                        except:
                            pass
        except:
            raise
            return
