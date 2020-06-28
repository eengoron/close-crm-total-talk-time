from flask import Flask, session
from app.methods import export_total_talk_time_per_lead_for_each_org
import logging
import os
from apscheduler.schedulers.blocking import BlockingScheduler

log = logging.getLogger('apscheduler.executors.default')
log.setLevel(logging.INFO)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)

sched = BlockingScheduler()

def export_job():
    export_total_talk_time_per_lead_for_each_org()
    print("Export of Talk Time Completed")


sched.add_job(export_job, trigger='cron', hour=os.environ.get('JOB_HOUR'), minute='00')

sched.start()  

app = Flask(__name__)

if __name__ == '__main__':
    app.run(use_reloader=False, debug=False)
