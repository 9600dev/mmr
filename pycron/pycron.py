import os
import sys
import logging
import coloredlogs
import click
import datetime as dt
import yaml
import asyncio
import psutil
import backoff
import json
import nest_asyncio
import socket

from crontab import CronTab
from typing import List, Dict, Optional, cast
from asyncio.subprocess import Process
from asyncio import Task

from tornado.platform.asyncio import AsyncIOMainLoop
from tornado.web import RequestHandler, Application, url

from rx.scheduler.eventloop.asynciothreadsafescheduler import AsyncIOThreadSafeScheduler

log = logging.getLogger('pycron')

class Job():
    def __init__(self, dictionary: Optional[Dict[str, str]]):
        self.name: str = ''
        self.description: str = ''
        self.command: str = ''
        self.arguments: str = ''
        self.working_directory: str = ''
        self.start: str = ''
        self.stop: str = ''
        self.trying_to_start = False
        self.finished: bool = False
        self.start_on_pycron_start: bool = False
        self.restart_if_found: bool = False
        self.restart_if_finished: bool = False
        self.start_count: int = 0
        self.running: bool = False
        self.pid: int = 0
        self.return_code: Optional[int] = None
        self.process: Optional[Process] = None
        self.eval: Optional[List[str]] = None
        self.eval_result: Optional[List[bool]] = None
        self.last_started: Optional[dt.datetime] = None
        self.depends_on: Optional[List[str]] = None
        self.delay: int = 0
        self.eval_running: Optional[List[str]] = None
        self.eval_running_result: Optional[List[bool]] = None

        if dictionary:
            for k, v in dictionary.items():
                setattr(self, k, v)

        self.crontab_start = CronTab(self.start)
        self.crontab_stop: Optional[CronTab] = None
        if self.stop:
            self.crontab_stop = CronTab(self.stop)

    def __str__(self):
        return '{} {} {} {} {}: {} {}'.format(self.name,
                                              self.command,
                                              self.arguments,
                                              self.pid,
                                              self.start_count,
                                              self.start,
                                              self.stop)

    def __dict__(self):
        return {
            'name': self.name,
            'description': self.description,
            'command': self.command,
            'arguments': self.arguments,
            'working_directory': self.working_directory,
            'start': self.start,
            'stop': self.stop,
            'trying_to_start': self.trying_to_start,
            'start_on_pycron_start': self.start_on_pycron_start,
            'running': self.running,
            'restart_if_found': self.restart_if_found,
            'restart_if_finished': self.restart_if_finished,
            'start_count': self.start_count,
            'finished': self.finished,
            'pid': self.pid,
            'eval': self.eval,
            'eval_result': self.eval_result,
            'depends_on': self.depends_on,
            'delay': self.delay,
            'eval_running': self.eval_running,
            'eval_running_result': self.eval_running_result,
        }

class JobScheduler():
    def __init__(self, jobs_list: Optional[List[Job]] = None, health_check_eval: Optional[List[str]] = None):
        self.jobs: List[Job] = []
        self.scheduler = AsyncIOThreadSafeScheduler(asyncio.get_event_loop())
        self.first_start = True
        self.polling_period = 60.0
        self.health_check_eval = health_check_eval
        if jobs_list:
            self.schedule_jobs(jobs_list)

    def health_check(self):
        eval_list = []
        # single element
        if type(self.health_check_eval) is str:
            self.health_check_eval = [self.health_check_eval]  # type: ignore

        # multiple lines
        if self.health_check_eval:
            eval_results = []
            for e in self.health_check_eval:
                eval_results.append(eval(e))
            return all(eval_results)
        # nothing to do
        else:
            return True

    def get_job(self, name: str) -> Optional[Job]:
        job = None
        for j in self.jobs:
            if j.name == name:
                job = j
        return job

    def get_jobs(self, names: List[str]) -> List[Job]:
        jobs = []
        for j in names:
            if self.get_job(j):
                jobs.append(cast(Job, self.get_job(j)))
        return jobs

    def is_running(self, job: Job) -> bool:
        if job.eval_running:
            if type(job.eval_running) is str:
                job.eval_running = [job.eval_running]  # type: ignore
            if job.eval_running:
                eval_running_results = []
                for e in job.eval_running:
                    eval_running_results.append(eval(e))
                job.eval_running_result = eval_running_results
                return all(job.eval_running_result)
            else:
                return True
        else:
            return job.running and not job.finished

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    async def start_job(self, job: Job):
        if job.trying_to_start:
            return

        if job.depends_on:
            log.info('trying to start job: {} that depends on: {}'.format(job, str(job.depends_on)))
        else:
            log.info('trying to start job: {}'.format(job))

        job.trying_to_start = True
        cwd = job.working_directory or os.getcwd()
        command = '{} {}'.format(job.command, job.arguments)

        if job.depends_on and not len(job.depends_on) == len(self.get_jobs(job.depends_on)):
            log.error('cannot find all dependent jobs {} for {}, aborting'.format(job.depends_on, job))
            job.trying_to_start = False
            return ''

        while job.depends_on and [j for j in self.get_jobs(job.depends_on) if not self.is_running(j)]:  # type: ignore
            # wait until it's running
            # todo do some backoff/exception stuff here
            await asyncio.sleep(5)

        # delay start (default is 0)
        await asyncio.sleep(job.delay)

        process = await asyncio.create_subprocess_shell(
            command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            cwd=cwd
        )
        job.trying_to_start = False
        job.process = process
        job.running = True
        job.finished = False
        job.pid = process.pid
        job.last_started = dt.datetime.now()

        stopping_debug = ''
        if job.crontab_stop:
            stopping_debug = 'stopping job in {} secs'.format(job.crontab_stop.next(default_utc=False))
        log.info('started job: {} {} {} {}'.format(job.command, job.arguments, str(process.pid), stopping_debug))

        stdout, stderr = await process.communicate()
        result = stdout.decode().strip()

        job.return_code = process.returncode
        job.running = False
        job.process = None
        job.pid = 0
        job.finished = True
        job.start_count += 1

        log.info('job finished ret: [{}] {} stdout: {} stderr: {}'.format(job.return_code,
                                                                          job.command,
                                                                          result,
                                                                          stderr.decode().strip()))
        return result

    # todo make this better
    def restart_job(self, job: Job):
        self.stop_job(job)
        task = [asyncio.create_task(self.start_job(job))]
        asyncio.gather(*task)

    def compare_process_name(self, process: psutil.Process, name: str, arguments: str):
        def compare_list(a: List[str], b: List[str]):
            return [s.lower() for s in a] == [s.lower() for s in b]

        process_names: List[str] = []
        process_names.append(name.lower())

        # strip command from arguments
        process_arguments = process.as_dict()['cmdline'][1:]
        args = arguments.split(' ')

        if os.path.exists(name):
            process_names.append(os.path.basename(name).lower())
        return process.name().lower() in process_names and compare_list(args, process_arguments)

    def stop_job(self, job: Job):
        def kill(proc_pid):
            try:
                process = psutil.Process(proc_pid)
                for proc in process.children(recursive=True):
                    proc.kill()
                process.kill()
                return True
            except Exception as ex:
                logging.error('kill() for {} failed with {}'.format(job, ex))
                return False

        def ps_kill(job: Job):
            try:
                for proc in psutil.process_iter():
                    if self.compare_process_name(proc, job.command, job.arguments):
                        log.debug('stop_job found process {}, kill()'.format(proc.name()))
                        return kill(proc.pid)
                return False
            except Exception as ex:
                logging.error('ps_kill() for {} failed with {}'.format(job, ex))

        if job.process:
            log.debug('stop_job process kill() {}'.format(job))
            if kill(job.process.pid):
                job.process = None
                job.running = False
                job.finished = True
                return

        log.debug('stop_job no process, trying to find using ps')
        if ps_kill(job):
            job.process = None
            job.running = False
            job.finished = True
            return
        log.error('was not able to kill {}'.format(job))

    def polling_loop(self):
        def recently_started_with_error(job: Job):
            return (
                job.return_code
                and job.return_code > 0
                and job.last_started
                and (dt.datetime.now() - job.last_started).seconds < self.polling_period * (job.start_count + 1)
            )

        def within_polling_period(cron: CronTab):
            return (
                cron.next(default_utc=False) <= self.polling_period       # type: ignore
                and cron.next(default_utc=False) >= -self.polling_period  # type: ignore
            )

        def test_eval(job: Job) -> bool:
            if type(job.eval) is str:
                job.eval = [job.eval]  # type: ignore
            if job.eval:
                eval_results = []
                for e in job.eval:
                    eval_results.append(eval(e))
                job.eval_result = eval_results
                return all(job.eval_result)
            else:
                return True

        tasks: List[Task] = []
        for job in self.jobs:
            # check to see if the process is already running
            # and if so, hook it up to the job
            if self.first_start and not job.running and not job.process:
                processes = list(psutil.process_iter())
                for proc in psutil.process_iter():
                    if self.compare_process_name(proc, job.command, job.arguments):
                        log.debug('found existing process {}, for job {}'.format(proc.name(), job))
                        job.process = proc  # type: ignore
                        job.pid = proc.pid
                        job.running = True

                        if job.restart_if_found:
                            log.debug('restart_if_found is true, restarting {}'.format(job))
                            self.stop_job(job)

            # check to see if there is an eval, and if so, evaluate it
            # if False, kill job if it exists, or skip
            if not test_eval(job):
                log.debug('eval for job {}, eval: {} was False'.format(job, job.eval))
                # check to see if the job is running, and if so, kill it
                if self.is_running(job):
                    log.debug('eval for job {} failed, stopping job'.format(job))
                    self.stop_job(job)
                continue

            # if we're firing up for the first time, schedule these jobs if
            # the job stop time is positive, or doesn't exist
            if (
                self.first_start and not self.is_running(job) and job.start_on_pycron_start
                and (not job.crontab_stop or (job.crontab_stop and job.crontab_stop.next(default_utc=False) > 0))  # type: ignore
            ):
                tasks.append(asyncio.create_task(self.start_job(job)))
            # try stop the job, irregardless of if we know if it's started or not
            elif (
                self.is_running(job) and job.crontab_stop and within_polling_period(job.crontab_stop)
            ):
                self.stop_job(job)
            # restart finished job
            elif (
                not self.is_running(job)
                and job.restart_if_finished
                and job.finished
                and not recently_started_with_error(job)
                and job.crontab_stop
                and job.crontab_start.next(default_utc=False) < job.crontab_stop.next(default_utc=False)  # type: ignore
            ):
                logging.debug('restarting finished job')
                tasks.append(asyncio.create_task(self.start_job(job)))
            # restart finished job
            elif (
                not self.is_running(job)
                and job.restart_if_finished
                and job.finished
                and not recently_started_with_error(job)
                and not job.crontab_stop
            ):
                logging.debug('restarting finished job, as no cron stop configured')
                tasks.append(asyncio.create_task(self.start_job(job)))
            # start the job
            elif (
                not self.is_running(job) and within_polling_period(job.crontab_start)
            ):
                tasks.append(asyncio.create_task(self.start_job(job)))

        self.first_start = False
        # wait on process start tasks
        asyncio.gather(*tasks)

    def schedule_job(self, job: Job):
        log.debug('scheduling job "{}"'.format(job))
        self.jobs.append(job)

    def schedule_jobs(self, jobs: List[Job]):
        for j in jobs:
            self.schedule_job(j)

    def start(self):
        log.debug('starting job scheduler with {} jobs'.format(len(self.jobs)))
        # start now
        self.scheduler.schedule(lambda x, y: self.polling_loop())
        # then schedule every 'n' periods
        self.scheduler.schedule_periodic(self.polling_period, lambda x: self.polling_loop())
        # schedule health check
        if self.health_check_eval:
            self.scheduler.schedule_periodic(self.polling_period * 10,
                                             lambda x: logging.info('health check: {}'.format(self.health_check())))


class MainHandler(RequestHandler):
    def initialize(self, job_scheduler: JobScheduler):
        self.job_scheduler = job_scheduler

    def prepare(self):
        header = "Content-Type"
        body = "application/json"
        self.set_header(header, body)

    def restart(self):
        restart_argument = self.get_query_arguments('restart')[0]
        job = self.job_scheduler.get_job(restart_argument)
        if job:
            self.write(json.dumps({'result': '{} {}'.format('restarting', restart_argument)}))
            self.job_scheduler.restart_job(job)
        else:
            self.write(json.dumps({'result': 'failed'}))

    def get(self):
        # restart job
        if len(self.get_query_arguments('restart')) > 0:
            self.restart()
        # get info
        else:
            self.write(json.dumps([ob.__dict__() for ob in self.job_scheduler.jobs]))


def main(config_file: str, only_list: List[str], except_list: List[str]):
    def get_network_ip() -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip

    log.debug('network address: {}'.format(get_network_ip()))
    log.debug('loading config file {}'.format(config_file))
    conf_file = open(config_file, 'r')
    config = yaml.load(conf_file, Loader=yaml.FullLoader)

    # change the working directory
    root_directory = config['root_directory']
    if root_directory and not os.path.exists(root_directory):
        log.error('root_directory {} not found'.format(root_directory))
    elif root_directory and os.path.exists(root_directory):
        os.chdir(root_directory)

    port = config['port']
    health_check_eval = config['health_check_eval']
    conf_file.close()

    jobs: List[Job] = []
    for j in config['jobs']:
        job = Job(j)
        if only_list and job.name not in only_list:
            continue
        if except_list and job.name in except_list:
            continue
        jobs.append(job)
        log.debug('adding job: {}'.format(job))

    # required for nested asyncio calls and avoids RuntimeError: This event loop is already running
    nest_asyncio.apply()
    AsyncIOMainLoop().make_current()
    job_scheduler = JobScheduler(jobs, health_check_eval=health_check_eval)

    app = Application([
        url(r"/", MainHandler, dict(job_scheduler=job_scheduler))
    ])

    log.debug('starting pycron at port {}'.format(port))
    app.listen(port)

    job_scheduler.start()
    log.debug('run_forever()')
    asyncio.get_event_loop().run_forever()


@click.command()
@click.option('--config', required=True, help='yaml configuration file')
@click.option('--start', '-s', multiple=True, required=False, help='process name(s) to start [repeat option for more]')
@click.option('--butnot', '-n', multiple=True, required=False, help='process names(s) not to start [repeat option for more]')
def bootstrap(config: str, start, butnot):
    coloredlogs.install(level='INFO')
    if not os.path.exists(config):
        log.error('config_file does not exist')
        sys.exit(1)

    main(config, start, butnot)


if __name__ == '__main__':
    bootstrap()
