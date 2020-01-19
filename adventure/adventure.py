# -*- coding: utf-8 -*-
import luigi


class HitQuestionMarkBlockTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget(f"data/block_content")

    def run(self):
        print("*********************************")
        print(f"Mario hits a ? block")
        print("*********************************")
        with self.output().open("w") as out_file:
            out_file.write("hammer")


class GetPowerUpTask(luigi.Task):
    def requires(self):
        return HitQuestionMarkBlockTask()

    def output(self):
        return luigi.LocalTarget("data/power_up")

    def run(self):
        print("*********************************")
        print(f"Mario obtains a power up...")
        print("*********************************")
        with self.input().open("r") as content_file:
            for line in content_file:
                power_up = line

        with self.output().open("w") as out_file:
            out_file.write(power_up)


class FindLuigiTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget("data/location")

    def run(self):
        print("*********************************")
        print(f"Mario finds for Luigi...")
        print("*********************************")
        with self.output().open("w") as out_file:
            out_file.write("Yoshi's Island")


class KillLuigiTask(luigi.Task):
    def requires(self):
        return {
            "power-up": GetPowerUpTask(),
            "location": FindLuigiTask(),
        }

    def output(self):
        return luigi.LocalTarget(f"data/luigi")

    def run(self):
        print("*********************************")
        print(f"Mario is killing Luigi!!")
        print("*********************************")
        with self.input()["power-up"].open("r") as power_up_file:
            for line in power_up_file:
                power_up = line

        with self.input()["location"].open("r") as location_file:
            for line in location_file:
                location = line

        with self.output().open("w") as out_file:
            out_file.write(f"Luigi has been killed in {location} with an {power_up}!")
