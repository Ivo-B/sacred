#!/usr/bin/env python
# coding=utf-8
from __future__ import division, print_function, unicode_literals

from datetime import datetime, timedelta
from sacred.observers import MongoObserver
import importlib.util

import gridfs
import os
import shutil
import sys


class MongoAssistant(object):
    def __init__(self, database, prefix='default'):
        self.db = database
        self.ex = None
        self.ex_path = None
        self.prefix = prefix
        self.runs = self.db[self.prefix].runs if prefix is not 'default' else self.db.runs
        self.version_policy = 'newer'

    def set_experiment(self):
        spec = importlib.util.spec_from_file_location(os.path.basename(self.ex_path).split('.')[0], self.ex_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.ex = getattr(module, 'ex')

    def get_runs_by_status(self, status):
        self.runs.find({'status': status})

    def mark_dead_runs(self):
        """
        Find and mark all dead runs.
        Find all runs with a RUNNING status but no heartbeat within the last
        minute and set their status to DIED.
        """
        a_minute_ago = datetime.now() - timedelta(minutes=1)
        self.runs.update_many(
            {'status': 'RUNNING', 'heartbeat': {'$lt': a_minute_ago}},
            {'$set': {'status': 'DIED'}}
        )

    def get_status(self):
        """Return a summary of how many runs are in each status."""
        self.mark_dead_runs()
        pipeline = [{'$group': {'_id': '$status', 'count': {'$sum': 1}}}]
        return {r['_id']: r['count'] for r in self.runs.aggregate(pipeline)}

    def get_run(self, criterion):
        return self.runs.find_one(criterion)

    def get_experiment_from_db(self, id):
        fs = gridfs.GridFS(self.db)  # assuming database name is 'sacred'
        run_entry = self.runs.find_one({'_id': id})  # Query by id
        file_name = run_entry['experiment']['sources'][0][0]
        #fs.get(run_entry['experiment']['sources'][0][1])
        # Make directory
        path = os.path.join('tmp', str(id))
        if not os.path.exists(path):
            os.makedirs(path)
        #with open(os.path.join(path, '__init__.py'), 'wb') as ini:
        #    pass
        with open(os.path.join(path, file_name), 'wb') as f:
            f.write(fs.get(run_entry['experiment']['sources'][0][1]).read())
            #    f.write(fs.get(run_entry['artifacts'][0]).read())
        return os.path.join(os.getcwd(), path, file_name)

    def _check_dependencies(self, ex_dep, run_dep, version_policy):
        from pkg_resources import parse_version
        ex_dep = [string.split('==') for string in ex_dep]
        run_dep = [string.split('==') for string in run_dep]
        ex_dep = {name: parse_version(ver) for name, ver in ex_dep}
        check_version = {
            'newer': lambda ex, name, b: name in ex and ex[name] >= b,
            'equal': lambda ex, name, b: name in ex and ex[name] == b,
            'exists': lambda ex, name, b: name in ex
        }[version_policy]
        for name, ver in run_dep:
            assert check_version(ex_dep, name, parse_version(ver)), \
                "{} mismatch: ex={}, run={}".format(name, ex_dep[name], ver)


    def _check_sources(self, ex_sources, run_sources):
        for ex_source, run_source in zip(ex_sources, run_sources):
            if type(ex_source[1]) != type(run_source[1]):
                ex_source = (os.path.basename(ex_source[0]), ex_source[1])
                # finde source in db by objectID
                run_source = self.db.fs.files.find_one({"_id": run_source[1]})
                run_source = (os.path.basename(run_source['filename']), run_source['md5'])
                # compare md5-value and name
                if not ex_source == run_source:
                    raise RuntimeError('Source files did not match: experiment:'
                                   ' {} [{}] != {} [{}] (run)'.format(
                                       ex_source[0], ex_source[1],
                                       run_source[0], run_source[1]))


    def _check_names(self, ex_name, run_name):
        if not ex_name == run_name:
            raise KeyError('experiment names did not match: experiment name ''{} != {} (run name)'.format(ex_name, run_name))


    def run_from_db(self, criterion, db_name, version_policy='newer'):
        # ex_info = self.ex.get_experiment_info()
        for trials in range(10):
            run = self.get_run(criterion)
            if run is None:
                raise IndexError('No scheduled run found')
            # load experiment from db
            self.ex_path = self.get_experiment_from_db(run['_id'])
            self.set_experiment()
            ex_info = self.ex.get_experiment_info()
            # verify the run
            self._check_names(ex_info['name'], run['experiment']['name'])
            self._check_sources(ex_info['sources'], run['experiment']['sources'])
            self._check_dependencies(ex_info['dependencies'], run['experiment']['dependencies'], version_policy)

            # set status to INITIALIZING to prevent others from
            # running the same Run.
            old_status = run['status']
            run['status'] = 'INITIALIZING'
            replace_summary = self.runs.replace_one({'_id': run['_id'], 'status': old_status}, replacement=run)
            if replace_summary.modified_count == 1:
                break  # we've successfully acquired a run
            # otherwise we've been too slow and should try again
        else:
            raise IndexError("Something went wrong. We've not been able to "
                             "acquire a run for 10 attempts.")

        # add a matching MongoObserver to the experiment and tell it to
        # overwrite the run
        #fs = gridfs.GridFS(self.db, collection=self.prefix)
        self.ex.observers.append(MongoObserver.create(overwrite=run, db_name=db_name))

        # run the experiment based on the run
        res = self.ex.run(run['command'], config_updates=run['config'])

        # set status to INITIALIZING to prevent others from
        # running the same Run.
        #old_status = run['status']
        #run['status'] = 'COMPLETED'
        #replace_summary = self.runs.replace_one({'_id': run['_id'], 'status': old_status}, replacement=run)
        # remove the extra observer
        self.ex.observers.pop()
        # remove tmp experiment file from disk and db
        shutil.rmtree(os.path.dirname(self.ex_path))
        self.db.fs.files.delete_one({"filename": self.ex_path})

        return res


from pymongo import MongoClient
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_mail_notification(subject, body):
    fromaddr = "ivo.baltruschat@gmail.com"
    toaddr = "im.baltruschat@icloud.com"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject

    body = body
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, "ZimbabWe89")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()

if __name__ == '__main__':
    client = MongoClient()
    db_name = 'ExID-031117'
    db = client[db_name]
    ma = MongoAssistant(database=db)
    print('Try to run experiment in queue')
    for key in ma.get_status().keys():
        if 'QUEUED' in key:
            run = ma.run_from_db({"status": key}, db_name)
            email_body = "Status of runs:\n{0}\n\n{1}".format(ma.get_status(), run.experiment_info)

    for key in ma.get_status().keys():
        if 'QUEUED' in key:
            sys.exit(1)
    sys.exit(-1)