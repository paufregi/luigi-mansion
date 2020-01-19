# -*- coding: utf-8 -*-
import luigi
import yaml

from datetime import datetime, timedelta

DATETIME_FORMAT = "%Y-%m-%dT%H%M"


class Schedule(luigi.WrapperTask):
    time = luigi.DateMinuteParameter(default=datetime.now())
    schedule_file = luigi.Parameter(default="schedule.yaml")

    def schedule(self):
        with open(file=self.schedule_file, mode="r") as file:
            schedule = yaml.safe_load(file)
        return schedule

    def requires(self):
        return [
            ScheduledTask(
                name=x["name"], time=calculate_schedule_time(self.time, x["schedule"]),
            )
            for x in self.schedule()
        ]


class ScheduledTask(luigi.Task):
    name = luigi.Parameter()
    time = luigi.DateMinuteParameter()

    def requires(self):
        return AppExternalTask(f"app/{self.name}")

    def output(self):
        output = f"data/{self.time.strftime(DATETIME_FORMAT)}/{self.name}/file"
        return luigi.LocalTarget(output)

    def run(self):
        print("*********************************")
        print(f"Executing {self.input().path}...")
        print("*********************************")
        with self.output().open(mode="w") as file:
            file.write(f"Executing {self.input().path}...")


class AppExternalTask(luigi.ExternalTask):
    program = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(path=self.program)


def calculate_schedule_time(dt, schedule):
    mon, tue, wed, thu, fri, sat, sun = range(7)

    def _find_day(d, day):
        d = d.replace(hour=0, minute=0, second=0, microsecond=0)
        offset = (d.weekday() - day) % 7
        return d - timedelta(days=offset)

    if schedule == "monday":
        return _find_day(dt, mon)
    if schedule == "tuesday":
        return _find_day(dt, tue)
    if schedule == "wednesday":
        return _find_day(dt, wed)
    if schedule == "thursday":
        return _find_day(dt, thu)
    if schedule == "friday":
        return _find_day(dt, fri)
    if schedule == "saturday":
        return _find_day(dt, sat)
    if schedule == "sunday":
        return _find_day(dt, sun)
    if schedule == "monthly":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if schedule == "daily":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if schedule == "hourly":
        return dt.replace(minute=0, second=0, microsecond=0)
    raise ValueError(f"Unknown schedule: {schedule}")
