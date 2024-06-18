import os
import numpy as np
import requests
import yaml
import pandas
import logging
import argparse
import inspect
from datetime import date
import uuid
import shutil
import json

from glob import glob
from pathlib import Path
from pandas import DataFrame
from dataclasses import dataclass

discord_channels = {
    "performance-result": "https://discord.com/api/webhooks/(тут был дискорд)",
    "perf-results": "https://discord.com/api/webhooks//(тут был дискорд)",
    "preprod1-night-results": "https://discord.com/api/webhooks/(тут был дискорд)"
}

links = {
    "pre-production-1.casino.softswiss.com": {
        "Grafana": "https://{тут была графана)/d/gatling/gatling?orgId=1&var-ds=Jmeter-Gatling&from={}&to={}",
        "Datadog": "https://{тут был датадог)/dashboard/i5z-ux9-sta/performance-testing-v2?tpl_var_cache[0]=pre-production-1-cache&tpl_var_db[0]=pre-production-1-postgres&tpl_var_host[0]=pre-production-1&tpl_var_hutch[0]=pre-production-1-hutch&tpl_var_jobs1-host[0]=pre-production-1&tpl_var_jobs2-host[0]=pre-production-1&tpl_var_service[0]=pre-production-1&from_ts={}&to_ts={}&live=false",
        "APM": "https://{тут был датадог)/apm/services/pre-production-1/operations/rack.request/resources?env=production&start={}&end={}&paused=true",
    },
    "pre-production-2.casino.p6m.tech": {
        "Grafana": "https://{тут была графана)/d/gatling/gatling?orgId=1&var-ds=Jmeter3-Gatling&from={}&to={}",
        "Datadog": "https://{тут был датадог)/dashboard/i5z-ux9-sta/performance-testing-v2?tpl_var_cache[0]=pre-production-2-cache&tpl_var_db[0]=pre-production-2-postgres&tpl_var_host[0]=pre-production-2&tpl_var_hutch[0]=pre-production-2-hutch&tpl_var_jobs1-host[0]=pre-production-2&tpl_var_jobs2-host[0]=pre-production-2&tpl_var_service[0]=pre-production-2&from_ts={}&to_ts={}&live=false",
        "APM": "https://{тут был датадог)/apm/services/pre-production-2/operations/rack.request/resources?env=production&start={}&end={}&paused=true",
    },
    "madrid-test.casino.p6m.tech": {
        "Grafana": "https://{тут была графана)/d/gatling/gatling?orgId=1&var-ds=Jmeter-Gatling&from={}&to={}",
        "Datadog": "https://{тут был датадог)/dashboard/i5z-ux9-sta/performance-testing-v2?tpl_var_cache[0]=madrid-test-cache&tpl_var_db[0]=madrid-test-postgres&tpl_var_host[0]=madrid-test&tpl_var_hutch[0]=madrid-test-hutch&tpl_var_jobs1-host[0]=madrid-test&tpl_var_jobs2-host[0]=madrid-test&tpl_var_service[0]=madrid-test&from_ts={}&to_ts={}&live=false",
        "APM": "https://{тут был датадог)/apm/services/madrid-test/operations/rack.request/resources?env=production&start={}&end={}&paused=true",
    },
    "stg-perf-1.casino.p6m.tech": {
            "Grafana": "https://{тут была графана)/d/gatling/gatling?orgId=1&var-ds=Jmeter-Gatling&from={}&to={}",
            "Datadog": "https://{тут был датадог)/dashboard/i5z-ux9-sta/performance-testing-v2?tpl_var_cache[0]=stg-perf-1-cache&tpl_var_db[0]=stg-perf-1-postgres&tpl_var_host[0]=stg-perf-1&tpl_var_hutch[0]=stg-perf-1-hutch&tpl_var_jobs1-host[0]=stg-perf-1&tpl_var_jobs2-host[0]=stg-perf-1&tpl_var_service[0]=stg-perf-1&from_ts={}&to_ts={}&live=false",
            "APM": "https://{тут был датадог)/apm/services/stg-perf-1/operations/rack.request/resources?env=production&start={}&end={}&paused=true",
    },
    "stg-perf-2.casino.p6m.tech": {
            "Grafana": "https://{тут была графана)/d/gatling/gatling?orgId=1&var-ds=Jmeter3-Gatling&from={}&to={}",
            "Datadog": "https://{тут был датадог)/dashboard/i5z-ux9-sta/performance-testing-v2?tpl_var_cache[0]=stg-perf-2-cache&tpl_var_db[0]=stg-perf-2-postgres&tpl_var_host[0]=stg-perf-2&tpl_var_hutch[0]=stg-perf-2-hutch&tpl_var_jobs1-host[0]=stg-perf-2&tpl_var_jobs2-host[0]=stg-perf-2&tpl_var_service[0]=stg-perf-2&from_ts={}&to_ts={}&live=false",
            "APM": "https://{тут был датадог)/apm/services/stg-perf-2/operations/rack.request/resources?env=production&start={}&end={}&paused=true",
    },
}


# aggregated results for the whole test
@dataclass
class OverallResult:
    samples: int
    errors: int
    rps: int
    pct99: int
    pct95: int
    pct75: int
    pct50: int
    # optional fields
    pct90: int = 0
    pct25: int = 0
    samples_check: bool = True
    errors_check: bool = True
    rps_check: bool = True
    pct99_check: bool = True
    pct95_check: bool = True
    pct90_check: bool = True
    pct75_check: bool = True
    pct50_check: bool = True
    pct25_check: bool = True

    # convert to csv string
    def to_csv(self, generator, header=True) -> str:
        header_line = ""
        if header:
            if generator == "jmeter":
                header_line = "samples,errors,rps,25pct,50pct,75pct,90pct,95pct,99pct"
            else:
                header_line = "samples,errors,rps,50pct,75pct,95pct,99pct"
        if generator == "jmeter":
            content = f"""{header_line}
                    {self.samples},{self.errors},{self.rps},{self.pct25},{self.pct50},{self.pct75},{self.pct90},{self.pct95},{self.pct99}
                    """
        else:
            content = f"""{header_line}
                    {self.samples},{self.errors},{self.rps},{self.pct50},{self.pct75},{self.pct95},{self.pct99}
                    """
        return inspect.cleandoc(content)

    # self - old test
    # other - new test
    # create comparison csv with delta between all metrics
    def to_csv_comparison(self, other, generator, header=True) -> str:
        header_line = ""
        if header:
            if generator == "jmeter":
                header_line = "samples_old,samples_new,samples_delta,errors_old,errors_new,errors_delta,rps_old,rps_new,rps_delta,25pct_old,25pct_new,25pct_delta,50pct_old,50pct_new,50pct_delta,75pct_old,75pct_new,75pct_delta,90pct_old,90pct_new,90pct_delta,95pct_old,95pct_new,95pct_delta,99pct_old,99pct_new,99pct_delta"
            else:
                header_line = "samples_old,samples_new,samples_delta,errors_old,errors_new,errors_delta,rps_old,rps_new,rps_delta,50pct_old,50pct_new,50pct_delta,75pct_old,75pct_new,75pct_delta,95pct_old,95pct_new,95pct_delta,99pct_old,99pct_new,99pct_delta"
        if generator == "jmeter":
            content = f"""{header_line}
                {self.samples},{other.samples},{self.samples - other.samples},{self.errors},{other.errors},{self.errors - other.errors},{self.rps},{other.rps},{self.rps - other.rps},{self.pct25},{other.pct25},{self.pct25 - other.pct25},{self.pct50},{other.pct50},{self.pct50 - other.pct50},{self.pct75},{other.pct75},{self.pct75 - other.pct75},{self.pct90},{other.pct90},{self.pct90 - other.pct90},{self.pct95},{other.pct95},{self.pct95 - other.pct95},{self.pct99},{other.pct99},{self.pct99 - other.pct99}
                """
        else:
            content = f"""{header_line}
                    {self.samples},{other.samples},{self.samples - other.samples},{self.errors},{other.errors},{self.errors - other.errors},{self.rps},{other.rps},{self.rps - other.rps},{self.pct50},{other.pct50},{self.pct50 - other.pct50},{self.pct75},{other.pct75},{self.pct75 - other.pct75},{self.pct95},{other.pct95},{self.pct95 - other.pct95},{self.pct99},{other.pct99},{self.pct99 - other.pct99}
                    """
        return inspect.cleandoc(content)

    # private method to get confluence markdown ok/ko emoji
    @staticmethod
    def __md_emoji(ch: bool):
        if ch:
            return "(/)"
        else:
            return "(x)"

    # private method to get discord markdown ok/ko emoji
    @staticmethod
    def __emoji(ch: bool):
        if ch:
            return ":green_circle:"
        else:
            return ":face_with_symbols_over_mouth:"

    # convert to confluence markdown table
    def to_md_table(self, generator) -> str:
        if generator == "jmeter":
            table = f"""
                    ||Metric||Actual in load test||SLO||Result||
                    ||requests count|{self.samples}|{cfg["count"]}|{self.__md_emoji(self.samples_check)}|
                    ||errors count|{self.errors}|{cfg["error_count"]}|{self.__md_emoji(self.errors_check)}|
                    ||RPS|{self.rps}|{cfg["rps"]}|{self.__md_emoji(self.rps_check)}|
                    ||99pct|{self.pct99}|{cfg["99pct"]}|{self.__md_emoji(self.pct99_check)}|
                    ||95pct|{self.pct95}|{cfg["95pct"]}|{self.__md_emoji(self.pct95_check)}|
                    ||90pct|{self.pct90}|{cfg["90pct"]}|{self.__md_emoji(self.pct90_check)}|
                    ||75pct|{self.pct75}|{cfg["75pct"]}|{self.__md_emoji(self.pct75_check)}|
                    ||50pct|{self.pct50}|{cfg["50pct"]}|{self.__md_emoji(self.pct50_check)}|
                    ||25pct|{self.pct25}|{cfg["25pct"]}|{self.__md_emoji(self.pct25_check)}|
                    """
        else:
            table = f"""
                    ||Metric||Actual in load test||SLO||Result||
                    ||requests count|{self.samples}|{cfg["count"]}|{self.__md_emoji(self.samples_check)}|
                    ||errors count|{self.errors}|{cfg["error_count"]}|{self.__md_emoji(self.errors_check)}|
                    ||RPS|{self.rps}|{cfg["rps"]}|{self.__md_emoji(self.rps_check)}|
                    ||99pct|{self.pct99}|{cfg["99pct"]}|{self.__md_emoji(self.pct99_check)}|
                    ||95pct|{self.pct95}|{cfg["95pct"]}|{self.__md_emoji(self.pct95_check)}|
                    ||75pct|{self.pct75}|{cfg["75pct"]}|{self.__md_emoji(self.pct75_check)}|
                    ||50pct|{self.pct50}|{cfg["50pct"]}|{self.__md_emoji(self.pct50_check)}|
                    """
        return inspect.cleandoc(table)

    # self - old test result
    # other - new test result
    # tolerance - allowable degradation, e.g. tolerance=0.95 means that 5% degradation is allowed
    # compare results and convert to confluence markdown table
    def to_md_table_comparison(self, other, generator, tolerance=0.95) -> str:
        if generator == "jmeter":
            table = f"""
        ||Metric||Old test||New test||SLO||Result||
        ||requests count|{self.samples}|{other.samples}|{cfg["count"]}|{self.__md_emoji(other.samples_check & ((self.samples / other.samples if other.samples else 1) > tolerance))}|
        ||errors count|{self.errors}|{other.errors}|{cfg["error_count"]}|{self.__md_emoji(other.errors_check & ((self.errors / other.errors if other.errors else 1) > tolerance))}|
        ||RPS|{self.rps}|{other.rps}|{cfg["rps"]}|{self.__md_emoji(other.rps_check & ((self.rps / other.rps if other.rps else 1) > tolerance))}|
        ||99pct|{self.pct99}|{other.pct99}|{cfg["99pct"]}|{self.__md_emoji(other.pct99_check & ((self.pct99 / other.pct99 if other.pct99 else 1) > tolerance))}|
        ||95pct|{self.pct95}|{other.pct95}|{cfg["95pct"]}|{self.__md_emoji(other.pct95_check & ((self.pct95 / other.pct95 if other.pct95 else 1) > tolerance))}|
        ||90pct|{self.pct90}|{other.pct90}|{cfg["90pct"]}|{self.__md_emoji(other.pct90_check & ((self.pct90 / other.pct90 if other.pct90 else 1) > tolerance))}|
        ||75pct|{self.pct75}|{other.pct75}|{cfg["75pct"]}|{self.__md_emoji(other.pct75_check & ((self.pct75 / other.pct75 if other.pct75 else 1) > tolerance))}|
        ||50pct|{self.pct50}|{other.pct50}|{cfg["50pct"]}|{self.__md_emoji(other.pct50_check & ((self.pct50 / other.pct50 if other.pct50 else 1) > tolerance))}|
        ||25pct|{self.pct25}|{other.pct25}|{cfg["25pct"]}|{self.__md_emoji(other.pct25_check & ((self.pct25 / other.pct25 if other.pct25 else 1) > tolerance))}|
                    """
        else:
            table = f"""
        ||Metric||Old test||New test||SLO||Result||
        ||requests count|{self.samples}|{other.samples}|{cfg["count"]}|{self.__md_emoji(other.samples_check & ((self.samples / other.samples if other.samples else 1) > tolerance))}|
        ||errors count|{self.errors}|{other.errors}|{cfg["error_count"]}|{self.__md_emoji(other.errors_check & ((self.errors / other.errors if other.errors else 1) > tolerance))}|
        ||RPS|{self.rps}|{other.rps}|{cfg["rps"]}|{self.__md_emoji(other.rps_check & ((self.rps / other.rps if other.rps else 1) > tolerance))}|
        ||99pct|{self.pct99}|{other.pct99}|{cfg["99pct"]}|{self.__md_emoji(other.pct99_check & ((self.pct99 / other.pct99 if other.pct99 else 1) > tolerance))}|
        ||95pct|{self.pct95}|{other.pct95}|{cfg["95pct"]}|{self.__md_emoji(other.pct95_check & ((self.pct95 / other.pct95 if other.pct95 else 1) > tolerance))}|
        ||75pct|{self.pct75}|{other.pct75}|{cfg["75pct"]}|{self.__md_emoji(other.pct75_check & ((self.pct75 / other.pct75 if other.pct75 else 1) > tolerance))}|
        ||50pct|{self.pct50}|{other.pct50}|{cfg["50pct"]}|{self.__md_emoji(other.pct50_check & ((self.pct50 / other.pct50 if other.pct50 else 1) > tolerance))}|
                    """
        return inspect.cleandoc(table)

    def to_msg(self, cfg, args) -> str:
        if args.generator == "jmeter":
            msg = f"""
                    :performing_arts: {cfg["description"]}
                    stand: ** {args.host} **
                    build: ** {args.build_url} **
                    {args.script_name}
                    {self.__emoji(self.samples_check)} request count: {self.samples} (sla: {cfg["count"]})
                    {self.__emoji(self.rps_check)} rps: {self.rps} (sla: {cfg["rps"]})
                    {self.__emoji(self.errors_check)} errors count: {self.errors} (sla: {cfg["error_count"]})
                    
                    {self.__emoji(self.pct99_check)} 99pct: {self.pct99} (sla: {cfg["99pct"]})
                    {self.__emoji(self.pct95_check)} 95pct: {self.pct95} (sla: {cfg["95pct"]})
                    {self.__emoji(self.pct90_check)} 90pct: {self.pct90} (sla: {cfg["90pct"]})
                    {self.__emoji(self.pct75_check)} 75pct: {self.pct75} (sla: {cfg["75pct"]})
                    {self.__emoji(self.pct50_check)} 50pct: {self.pct50} (sla: {cfg["50pct"]})
                    {self.__emoji(self.pct25_check)} 25pct: {self.pct25} (sla: {cfg["25pct"]})
                    """
        else:
            msg = f"""
                    :performing_arts: {cfg["description"]}
                    stand: ** {args.host} **
                    build: ** {args.build_url} **
                    {args.script_name}
                    {self.__emoji(self.samples_check)} request count: {self.samples} (sla: {cfg["count"]})
                    {self.__emoji(self.rps_check)} rps: {self.rps} (sla: {cfg["rps"]})
                    {self.__emoji(self.errors_check)} errors count: {self.errors} (sla: {cfg["error_count"]})
                    
                    {self.__emoji(self.pct99_check)} 99pct: {self.pct99} (sla: {cfg["99pct"]})
                    {self.__emoji(self.pct95_check)} 95pct: {self.pct95} (sla: {cfg["95pct"]})
                    {self.__emoji(self.pct75_check)} 75pct: {self.pct75} (sla: {cfg["75pct"]})
                    {self.__emoji(self.pct50_check)} 50pct: {self.pct50} (sla: {cfg["50pct"]})
                    """
        return inspect.cleandoc(msg)


@dataclass
class Result:
    label: str
    success: int
    rps: int
    min: int
    pct50: int
    pct75: int
    pct95: int
    pct99: int
    max: int
    std_dev: int
    percent: int = 0
    pct25: int = 0
    pct90: int = 0
    pct98: int = 0

    def to_csv(self, generator) -> str:
        if generator == "jmeter":
            return f"{self.label},{self.success},{self.rps},{self.percent},{self.min},{self.pct25},{self.pct50},{self.pct75},{self.pct90},{self.pct95},{self.pct98},{self.pct99},{self.max},{self.std_dev}"
        else:
            return f"{self.label},{self.success},{self.rps},{self.min},{self.pct50},{self.pct75},{self.pct95},{self.pct99},{self.max},{self.std_dev}"

    def to_csv_comparison(self, other, generator) -> str:
        if generator == "jmeter":
            return f"{self.label},{self.success},{other.success},{self.success - other.success},{self.rps},{other.rps},{self.rps - other.rps},{self.percent},{other.percent},{self.percent - other.percent},{self.min},{other.min},{self.min - other.min},{self.pct25},{other.pct25},{self.pct25 - other.pct25},{self.pct50},{other.pct50},{self.pct50 - other.pct50},{self.pct75},{other.pct75},{self.pct75 - other.pct75},{self.pct90},{other.pct90},{self.pct90 - other.pct90},{self.pct95},{other.pct95},{self.pct95 - other.pct95},{self.pct98},{other.pct98},{self.pct98 - other.pct98},{self.pct99},{other.pct99},{self.pct99 - other.pct99},{self.max},{other.max},{self.max - other.max},{self.std_dev},{other.std_dev},{self.std_dev - other.std_dev}"
        else:
            return f"{self.label},{self.success},{other.success},{self.success - other.success},{self.rps},{other.rps},{self.rps - other.rps},{self.min},{other.min},{self.min - other.min},{self.pct50},{other.pct50},{self.pct50 - other.pct50},{self.pct75},{other.pct75},{self.pct75 - other.pct75},{self.pct95},{other.pct95},{self.pct95 - other.pct95},{self.pct99},{other.pct99},{self.pct99 - other.pct99},{self.max},{other.max},{self.max - other.max},{self.std_dev},{other.std_dev},{self.std_dev - other.std_dev}"

    def to_md_table(self, generator) -> str:
        if generator == "jmeter":
            return f"||{self.label}|{self.success}|{self.rps}|{self.percent}|{self.min}|{self.pct25}|{self.pct50}|{self.pct75}|{self.pct90}|{self.pct95}|{self.pct98}|{self.pct99}|{self.max}|{self.std_dev}|"
        else:
            return f"||{self.label}|{self.success}|{self.rps}|{self.min}|{self.pct50}|{self.pct75}|{self.pct95}|{self.pct99}|{self.max}|{self.std_dev}|"

    def to_md_table_comparison(self, other, generator) -> str:
        if generator == "jmeter":
            return f"||{self.label}|{self.success}|{other.success}|{self.success - other.success}|{self.rps}|{other.rps}|{self.rps - other.rps}|{self.percent}|{other.percent}|{self.percent - other.percent}|{self.min}|{other.min}|{self.min - other.min}|{self.pct25}|{other.pct25}|{self.pct25 - other.pct25}|{self.pct50}|{other.pct50}|{self.pct50 - other.pct50}|{self.pct75}|{other.pct75}|{self.pct75 - other.pct75}|{self.pct90}|{other.pct90}|{self.pct90 - other.pct90}|{self.pct95}|{other.pct95}|{self.pct95 - other.pct95}|{self.pct98}|{other.pct98}|{self.pct98 - other.pct98}|{self.pct99}|{other.pct99}|{self.pct99 - other.pct99}|{self.max}|{other.max}|{self.max - other.max}|{self.std_dev}|{other.std_dev}|{self.std_dev - other.std_dev}|"
        else:
            return f"||{self.label}|{self.success}|{other.success}|{self.success - other.success}|{self.rps}|{other.rps}|{self.rps - other.rps}|{self.min}|{other.min}|{self.min - other.min}|{self.pct50}|{other.pct50}|{self.pct50 - other.pct50}|{self.pct75}|{other.pct75}|{self.pct75 - other.pct75}|{self.pct95}|{other.pct95}|{self.pct95 - other.pct95}|{self.pct99}|{other.pct99}|{self.pct99 - other.pct99}|{self.max}|{other.max}|{self.max - other.max}|{self.std_dev}|{other.std_dev}|{self.std_dev - other.std_dev}|"


def results_to_csv(results, generator) -> str:
    if generator == "jmeter":
        content = "Label,Success,RPS,Percent,Min,25pct,50pct,75pct,90pct,95pct,98pct,99pct,Max,Std.dev\n"
    else:
        content = "Label,Success,RPS,Min,50pct,75pct,95pct,99pct,Max,Std.dev\n"
    for result in results:
        content += result.to_csv(generator) + '\n'
    return content


def results_to_csv_comparison(results_old, results_new, generator) -> str:
    if generator == "jmeter":
        content = "Label,Success_old,Success_new,Success_delta,RPS_old,RPS_new,RPS_delta," \
                  "Percent_old,Percent_new,Percent_delta,Min_old,Min_new,Min_delta,25pct_old,25pct_new,25pct_delta," \
                  "50pct_old,50oct_new,50pct_delta,75pct_old,75pct_new,75pct_delta,90pct_old,90pct_new,90pct_delta," \
                  "95pct_old,95pct_new,95pct_delta,98pct_old,98pct_new,98pct_delta,99pct_old,99pct_new,99pct_delta," \
                  "Max_old,Max_new,Max_delta,Std.dev_old,Std.dev_new,Std_dev_delta\n"
    else:
        content = "Label,Success_old,Success_new,Success_delta,RPS_old,RPS_new,RPS_delta," \
                  "Min_old,Min_new,Min_delta," \
                  "50pct_old,50oct_new,50pct_delta,75pct_old,75pct_new,75pct_delta," \
                  "95pct_old,95pct_new,95pct_delta,99pct_old,99pct_new,99pct_delta," \
                  "Max_old,Max_new,Max_delta,Std.dev_old,Std.dev_new,Std_dev_delta\n"
    for old in results_old:
        for new in results_new:
            if old.label == new.label:
                content += old.to_csv_comparison(new, generator) + '\n'
    return content


def results_to_md_table(results, generator) -> str:
    if generator == "jmeter":
        table = "||Label||Success||RPS||Percent||Min||25pct||50pct||75pct||90pct||95pct||98pct||99pct||Max||Std.dev||\n"
    else:
        table = "||Label||Success||RPS||Min||50pct||75pct||90pct||95pct||99pct||Max||Std.dev||\n"
    for result in results:
        table += result.to_md_table(generator) + '\n'
    return table


def results_to_md_table_comparison(results_old, results_new, generator) -> str:
    if generator == "jmeter":
        table = "||Label||Success_old||Success_new||Success_delta||RPS_old||RPS_new||RPS_delta||" \
                "Percent_old||Percent_new||Percent_delta||Min_old||Min_new||Min_delta||" \
                "25pct_old||25pct_new||25pct_delta||50pct_old||50oct_new||50pct_delta||75pct_old||75pct_new||75pct_delta||" \
                "90pct_old||90pct_new||90pct_delta||95pct_old||95pct_new||95pct_delta||98pct_old||98pct_new||98pct_delta||" \
                "99pct_old||99pct_new||99pct_delta||Max_old||Max_new||Max_delta||Std.dev_old||Std.dev_new||Std_dev_delta\n"
    else:
        table = "||Label||Success_old||Success_new||Success_delta||RPS_old||RPS_new||RPS_delta||" \
                "Min_old||Min_new||Min_delta||" \
                "50pct_old||50oct_new||50pct_delta||75pct_old||75pct_new||75pct_delta||" \
                "95pct_old||95pct_new||95pct_delta||" \
                "99pct_old||99pct_new||99pct_delta||Max_old||Max_new||Max_delta||Std.dev_old||Std.dev_new||Std_dev_delta\n"
    for old in results_old:
        for new in results_new:
            if old.label == new.label:
                table += old.to_md_table_comparison(new, generator) + '\n'
    return table


def init_logging(level):
    logging.basicConfig(
        level=level,
        format=u'%(filename)s[LINE:%(lineno)d]#%(levelname)-3s [%(asctime)s] %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()


def init_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--result_log', help="path to jtl result")
    parser.add_argument('-a', '--sla', help="path to sla.yml file")
    parser.add_argument('-n', '--script_name', help="script name for discord notification")
    parser.add_argument('-s', '--silence', default=False, type=lambda x: (str(x).lower() == 'true'))
    parser.add_argument('-t', '--host', help="target host", default="pre-production-1.casino.softswiss.com")
    parser.add_argument('-u', '--build_url', help="jenkins or gitlab build url", default="NONE BUILD URL")
    parser.add_argument('-c', '--compare', help="jenkins build number to compare with", default="None")
    parser.add_argument('-d', '--channel', help="discord channel", default="performance-result")
    parser.add_argument('-g', '--generator', help="generator tool: jmeter or gatling", default="jmeter")
    parser.add_argument('--start', help="start timestamp unix ms", default="None")
    parser.add_argument('--stop', help="stop timestamp unix ms", default="None")
    return parser.parse_args()


def read_sla_config(path):
    with open(path, 'r') as ymlfile:
        return yaml.load(ymlfile, Loader=yaml.FullLoader)

# TODO: add function for gatling simulation.log format
def calculate_overall_result(cfg, df):
    error_count = int(df['success'].loc[df['success'] == False].count())
    success_count = int(df['success'].loc[df['success'] == True].count())
    el = df.loc[df['success'] == True]
    rps = int(success_count / ((df["timeStamp"].max() - df["timeStamp"].min()) / 1000))
    pct99 = int(el['elapsed'].quantile(q=0.99))
    pct95 = int(el['elapsed'].quantile(q=0.95))
    pct90 = int(el['elapsed'].quantile(q=0.90))
    pct75 = int(el['elapsed'].quantile(q=0.75))
    pct50 = int(el['elapsed'].quantile(q=0.50))
    pct25 = int(el['elapsed'].quantile(q=0.25))
    return OverallResult(
        samples=success_count + error_count,
        errors=error_count,
        rps=rps,
        pct99=pct99,
        pct95=pct95,
        pct90=pct90,
        pct75=pct75,
        pct50=pct50,
        pct25=pct25,
        samples_check=int(cfg["count"]) <= success_count,
        errors_check=int(cfg["error_count"]) >= error_count,
        rps_check=int(cfg["rps"]) <= rps,
        pct99_check=int(cfg["99pct"]) >= pct99,
        pct95_check=int(cfg["95pct"]) >= pct95,
        pct90_check=int(cfg["90pct"]) >= pct90,
        pct75_check=int(cfg["90pct"]) >= pct75,
        pct50_check=int(cfg["75pct"]) >= pct50,
        pct25_check=int(cfg["25pct"]) >= pct25
    )


# TODO: add function for gatling simulation.log format
def calculate_result_by_label(df):
    test_duration = (df["timeStamp"].max() - df["timeStamp"].min()) / 1000
    labels_list = df['label'].unique()
    ok = df[df['success'] == True]
    ok = ok[ok['label'].isin(labels_list)]
    ok['timeStamp'] = ok['timeStamp'].round()
    ok_sample = ok.pivot_table(
        columns=['label'], index='timeStamp', values='success', aggfunc=np.sum)
    ok_elapsed = ok.pivot_table(
        columns=['label'], index='timeStamp', values='elapsed', aggfunc=np.mean)
    request_count = df["success"].sum()
    results = []
    for label in labels_list:
        results.append(Result(
            label=label,
            success=int(ok_sample[label].sum()),
            rps=int(ok_sample[label].sum() / test_duration),
            percent=int(round(ok_sample[label].sum() / request_count * 100, 4)),
            min=int(ok_elapsed[label].min()),
            pct25=int(ok_elapsed[label].quantile(0.25)),
            pct50=int(ok_elapsed[label].quantile(0.50)),
            pct75=int(ok_elapsed[label].quantile(0.75)),
            pct90=int(ok_elapsed[label].quantile(0.90)),
            pct95=int(ok_elapsed[label].quantile(0.95)),
            pct98=int(ok_elapsed[label].quantile(0.98)),
            pct99=int(ok_elapsed[label].quantile(0.99)),
            max=int(ok_elapsed[label].max()),
            std_dev=int(ok_elapsed[label].std())
        ))
    return results


def dump_results_to_csv(result, path, build_number, date_str, generator):
    path = Path(path)
    if type(result) is OverallResult:
        path.mkdir(parents=True, exist_ok=True)
        path = path / "results.csv"
        if path.is_file():
            content = f"\n{build_number},{date_str},{result.to_csv(generator, header=False)}"
            write_mode = "a"
        else:
            content = f"launch,date,{result.to_csv(generator, header=True)}"
            content = content.replace("\n", f"\n{build_number},{date_str},")
            write_mode = "w+"
    else:
        path = path / "results_by_label"
        path.mkdir(parents=True, exist_ok=True)
        path = path / f"{build_number}_{date_str}.csv"
        content = results_to_csv(result, generator)
        write_mode = "w+"

    with open(path, write_mode) as f:
        f.write(content)
    return path


# TODO: is there another way of reading csv into a dataclass?
def load_from_csv(path, result_type, build_number, generator):
    if result_type == "overall":
        path = Path(path)
        path = path / "results.csv"
        with open(path, "r") as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith(str(build_number)):
                overall_result_str = line
                r = overall_result_str.split(",")
                if generator == "jmeter":
                    return OverallResult(
                        samples=int(round(float(r[2]))),
                        errors=int(round(float(r[3]))),
                        rps=int(round(float(r[4]))),
                        pct25=int(round(float(r[5]))),
                        pct50=int(round(float(r[6]))),
                        pct75=int(round(float(r[7]))),
                        pct90=int(round(float(r[8]))),
                        pct95=int(round(float(r[9]))),
                        pct99=int(round(float(r[10])))
                    )
                else:
                    return OverallResult(
                        samples=int(round(float(r[2]))),
                        errors=int(round(float(r[3]))),
                        rps=int(round(float(r[4])))
                        # ,
                        # pct50=int(round(float(r[5]))),
                        # pct75=int(round(float(r[6]))),
                        # pct95=int(round(float(r[7]))),
                        # pct99=int(round(float(r[8])))
                    )

    elif result_type == "by_label":
        path = Path(glob(path + "/results_by_label/" + build_number + "_*.csv")[0])
        results = list()
        with open(path, "r") as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith("Label"):
                continue
            r = line.split(",")
            if generator == "jmeter":
                results.append(Result(
                    label=r[0],
                    success=int(round(float(r[1]))),
                    rps=int(round(float(r[2]))),
                    percent=int(round(float(r[3]))),
                    min=int(round(float(r[4]))),
                    pct25=int(round(float(r[5]))),
                    pct50=int(round(float(r[6]))),
                    pct75=int(round(float(r[7]))),
                    pct90=int(round(float(r[8]))),
                    pct95=int(round(float(r[9]))),
                    pct98=int(round(float(r[10]))),
                    pct99=int(round(float(r[11]))),
                    max=int(round(float(r[12]))),
                    std_dev=int(round(float(r[13])))
                ))
            else:
                results.append(Result(
                    label=r[0],
                    success=int(round(float(r[1]))),
                    rps=int(round(float(r[2]))),
                    min=int(round(float(r[3]))),
                    pct50=int(round(float(r[4]))),
                    pct75=int(round(float(r[5]))),
                    pct95=int(round(float(r[6]))),
                    pct99=int(round(float(r[7]))),
                    max=int(round(float(r[8]))),
                    std_dev=int(round(float(r[9])))
                ))
        return results


def load_gatling_global_stats(cfg, path):
    path = Path(path) / "global_stats.json"
    with open(path, 'r') as jsonfile:
        stats = json.load(jsonfile)
        samples = int(stats['numberOfRequests']['total'])
        errors = int(stats['numberOfRequests']['ko'])
        rps = int(stats['meanNumberOfRequestsPerSecond']['total'])
        pct50 = int(stats['percentiles1']['ok'])
        pct75 = int(stats['percentiles2']['ok'])
        pct95 = int(stats['percentiles3']['ok'])
        pct99 = int(stats['percentiles4']['ok'])
        return OverallResult(
            samples=samples,
            errors=errors,
            rps=rps,
            pct50=pct50,
            pct75=pct75,
            pct95=pct95,
            pct99=pct99,
            samples_check=int(cfg["count"]) <= samples,
            errors_check=int(cfg["error_count"]) >= errors,
            rps_check=int(cfg["rps"]) <= rps,
            pct99_check=int(cfg["99pct"]) >= pct99,
            pct95_check=int(cfg["95pct"]) >= pct95,
            pct75_check=int(cfg["90pct"]) >= pct75,
            pct50_check=int(cfg["75pct"]) >= pct50,
        )


def load_gatling_stats(path):
    path = Path(path) / "stats.json"
    results = list()
    with open(path, 'r') as jsonfile:
        stats = json.load(jsonfile)
    for key, value in stats['contents'].items():
        results.append(Result(
            label=value['stats']['name'],
            success=int(value['stats']['numberOfRequests']['ok']),
            rps=int(value['stats']['meanNumberOfRequestsPerSecond']['ok']),
            min=int(value['stats']['minResponseTime']['ok']),
            pct50=int(value['stats']['percentiles1']['ok']),
            pct75=int(value['stats']['percentiles2']['ok']),
            pct95=int(value['stats']['percentiles3']['ok']),
            pct99=int(value['stats']['percentiles4']['ok']),
            max=int(value['stats']['maxResponseTime']['ok']),
            std_dev=int(value['stats']['standardDeviation']['ok'])
        ))
    return results


def discord_post_message(content, url):
    data = {
        "content": "{}".format(content),
        "username": "finish test {}".format(args.script_name)
    }
    result = requests.post(url, json=data)
    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as err:
        return f"failed to send discord message: {err}"
    else:
        return f"message sent: {result.status_code}"


def format_links(host, links, start, stop):
    formatted_links = dict()
    for name, link in links[host].items():
        link = link.format(start, stop)
        formatted_links[name] = link
    return formatted_links


if __name__ == '__main__':
    log = init_logging(logging.DEBUG)

    log.info("reading cli args")
    args = init_args()

    log.info("loading slo config: %s", args.sla)
    cfg = read_sla_config(args.sla)

    if args.generator == "jmeter":
        log.info("reading raw jmeter results into dataframe: %s", args.result_log)
        df = DataFrame()
        try:
            df = pandas.read_csv(args.result_log, delimiter=',', low_memory=False)
        except Exception as e:
            log.error("failed to parse result.jtl:\n%s", e)
            # fatal error
            exit(1)
        log.info("start analysing results")
        overall_result = calculate_overall_result(cfg, df)
        result_by_label = calculate_result_by_label(df)

    elif args.generator == "gatling":
        log.info("reading gatling stats from: %s", args.result_log)
        overall_result = load_gatling_global_stats(cfg, args.result_log)
        result_by_label = load_gatling_stats(args.result_log)
    else:
        log.error("unknown generator type: %s", args.generator)
        exit(1)

    log.info("overall results markdown table:\n%s\n", overall_result.to_md_table(args.generator))
    log.info("overall results csv table:\n%s\n", overall_result.to_csv(args.generator))

    log.info("results by label markdown table:\n%s\n", results_to_md_table(result_by_label, args.generator))
    log.info("results by label csv table:\n%s\n", results_to_csv(result_by_label, args.generator))

    if not args.silence:
        content = overall_result.to_msg(cfg, args)
        if args.start != "None" and args.stop != "None":
            formatted_links = format_links(args.host, links, args.start, args.stop)
            content += "\n\n"
            for name, link in formatted_links.items():
                content += f"{name}: {link}\n"
        log.info("notify: #%s\n%s", args.channel, content)
        response = discord_post_message(content, discord_channels[args.channel])
        log.info(response)

    today = date.today()
    date_str = today.strftime('%d-%m-%Y')

    build_id = os.getenv("BUILD_ID",
                         uuid.uuid4())  # need something uniq for local debug purpose so let it be uuid
    if args.generator == "jmeter":
        # get predefined jenkins variables
        job_name = os.getenv("JOB_NAME", "debug")
        store_result_dir = f"/home/jmeter/results/{job_name}"
        log.info("dump jmeter results to csv: %s", store_result_dir)
    else:
        job_name = args.host + "/" + args.script_name
        store_result_dir = f"/results/{job_name}"
        log.info("dump gatling results to csv: %s", store_result_dir)

    overall_path = dump_results_to_csv(overall_result, store_result_dir, build_id, date_str, args.generator)
    log.info("overall result saved to: %s", overall_path)
    by_label_path = dump_results_to_csv(result_by_label, store_result_dir, build_id, date_str, args.generator)
    log.info("by label results saved to: %s", by_label_path)
    log.info("results saved")

    # TODO: compare any two tests
    if args.compare != "None":
        log.info("compare current test with %s/%s", job_name, args.compare)

        log.info("restore %s test overall results", args.compare)
        old_overall_result = load_from_csv(store_result_dir, "overall", args.compare, args.generator)
        log.info("overall result comparison markdown table:\n%s\n",
                 old_overall_result.to_md_table_comparison(overall_result, args.generator, tolerance=0.95))
        log.info("overall result comparison csv table:\n%s\n",
                 old_overall_result.to_csv_comparison(overall_result, args.generator))

        log.info("restore %s test results by label", args.compare)
        old_result_by_label = load_from_csv(store_result_dir, "by_label", args.compare, args.generator)
        log.info("result by label comparison markdown table:\n%s\n",
                 results_to_md_table_comparison(old_result_by_label, result_by_label, args.generator))
        log.info("result by label comparison csv table:\n%s\n",
                 results_to_csv_comparison(old_result_by_label, result_by_label, args.generator))

    # archive jmeter log
    if args.generator == "jmeter":
        # save raw jtl
        raw = Path(args.result_log)
        arch = Path(f"{store_result_dir}/arch")
        arch.mkdir(parents=True, exist_ok=True)
        arch = arch / f"{build_id}_{date_str}.jtl"
        shutil.copy(raw, arch)
