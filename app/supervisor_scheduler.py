import logging
import shlex
import time
from datetime import datetime
from subprocess import Popen, STDOUT, PIPE


def is_running(name) -> bool:
    p = Popen(shlex.split(f'supervisorctl status {name}'), stdout=PIPE, stderr=STDOUT)
    stdout, stderr = p.communicate(timeout=30)
    return True if 'running' in stdout.decode().lower() else False


def start(name) -> bool:
    p = Popen(shlex.split(f'supervisorctl start {name}'), stdout=PIPE, stderr=STDOUT)
    stdout, stderr = p.communicate(timeout=30)
    return True if 'started' in stdout.decode().lower() else False


def scheduler():
    global main_started, helper_started
    now = datetime.now()

    if now.hour == start_hour and now.minute in range(0, 5):
        name = Services['main']
        if not main_started:
            if start(name):
                main_started = True
                logging.info(f"{name} started successful")
            else:
                logging.error(f"{name} is not started")
                time.sleep(60)
    else:
        main_started = False

    if now.hour == start_hour and now.minute in range(10, 15):
        if not helper_started:
            s = dict()
            for i in range(Services['nprocs']):
                name = f"{Services['helper']}:{i:02d}"
                if start(name):
                    s['action'] = True
                    logging.info(f"{name} started successful")
                else:
                    logging.error(f"{name} is not started")
                    time.sleep(60)
            if False not in s:
                helper_started = True
            else:
                logging.error(f"Not all actions started: {s}")
    else:
        helper_started = False


main_started = False
helper_started = False
start_hour = 14

Services = dict(main='main_mode', helper='helper_mode', nprocs=4)

while True:
    scheduler()
    time.sleep(10)
