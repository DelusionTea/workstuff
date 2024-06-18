from datetime import datetime

import numpy as np
import pandas
import argparse

import requests
import yaml

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("result_log", help="path to jtl result")
    parser.add_argument("script_name", help="asdf")
    parser.add_argument("slo", help=" ")
    parser.add_argument('--silence', dest='silence', help="suppress discord notifications", action='store_true')
    parser.set_defaults(silence=False)
    args = parser.parse_args()

    df = pandas.read_csv(args.result_log, delimiter=',')

    with open(args.slo, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    test_duration = (df["timeStamp"].max() - df["timeStamp"].min()) / 1000
    table_results = ""
    table_results += "test start: {}\ntest end: {}\ntest duration: {}".format(
        df["timeStamp"].min(),
        df["timeStamp"].max(),
        test_duration)

    df_error = df['success'].loc[df['success'] == False].count()
    print("df error: {}".format(df_error))
    table_results += "df error: {}".format(df_error)

    df_success = df['success'].loc[df['success'] == True].count()
    print("df success: {}".format(df_success))
    table_results += "df success: {}".format(df_success)

    rps = int(df_success / ((df["timeStamp"].max() - df["timeStamp"].min()) / 1000))
    table_results += "rps: {}".format(rps)

    labels_list = df['label'].unique()

    ok = df[df['success'] == True]
    ok['timeStamp_round'] = [round(a / 1) * 1 for a in ok.index]
    filter_request = ok['label'].isin(labels_list)
    ok = ok[filter_request]

    ok_sample = ok.pivot_table(
        columns=['label'], index='timeStamp_round', values='success', aggfunc=np.sum)

    ok_elapsed = ok.pivot_table(
        columns=['label'], index='timeStamp_round', values='elapsed', aggfunc=np.mean)

    request_count = df["success"].sum()

    table_results += "\n\n parsing result test:\n"


    def get_beautiful_label(label):
        eee = label.replace(" ", "_")
        eee = eee.replace("/", "_")
        eee = eee.replace("__", "_")
        return eee


    for label in labels_list:
        eee = get_beautiful_label(label)

        table_results += "\n{}:\n  rps: {}\n  min: {}\n  25pct: {}\n  50pct: {}\n  75pct: {}\n  90pct: " \
                         "{}\n  95pct: {}\n  98pct: {}\n  99pct: {}\n  max: {}".format(
            eee,
            int(ok_sample[label].sum() / test_duration),
            int(ok_elapsed[label].min()),
            int(ok_elapsed[label].quantile(0.25)),
            int(ok_elapsed[label].quantile(0.50)),
            int(ok_elapsed[label].quantile(0.75)),
            int(ok_elapsed[label].quantile(0.90)),
            int(ok_elapsed[label].quantile(0.95)),
            int(ok_elapsed[label].quantile(0.98)),
            int(ok_elapsed[label].quantile(0.99)),
            int(ok_elapsed[label].max()))


    def get_percent(val_new, val_old):
        try:
            return int(val_new / val_old * 100 - 100)
        except:
            return "null"


    table_results += "\n\n-----SLO CHECK-------\n"

    el = df.loc[df['success'] == True]


    def checker(val, min, max):
        if min > int(val):
            check = ":high_brightness:"
        elif max < int(val):
            check = ":face_with_symbols_over_mouth:"
        else:
            check = ":green_circle:"
        return str(check)


    def checker2(val, min, max):
        return ":face_with_symbols_over_mouth:" if min < int(
            val) or max > int(val) else ":green_circle:"


    d2 = checker(8, 9, 12)
    message_slo_requests = ""

    for label in labels_list:
        eee = get_beautiful_label(label)

        rps_label = int(ok_sample[label].sum() / test_duration)

        try:
            message_slo_requests += "\n{}:" \
                                    "\n{} 50pct: {} ({} {}%)" \
                                    "\n{} 75pct: {} ({} {}%)" \
                                    "\n{} 90pct: {} ({} {}%)" \
                                    "\n{} 95pct: {} ({} {}%)".format(
                eee,

                checker(ok_elapsed[label].quantile(0.50), int(cfg[eee]["50pct_up"]), int(cfg[eee]["50pct_down"])),
                int(ok_elapsed[label].quantile(0.50)), int(cfg[eee]["50pct"]),
                get_percent(ok_elapsed[label].quantile(0.50), cfg[eee]["50pct"]),

                checker(ok_elapsed[label].quantile(0.75), int(cfg[eee]["75pct_up"]), int(cfg[eee]["75pct_down"])),
                int(ok_elapsed[label].quantile(0.75)), cfg[eee]["75pct"],
                get_percent(ok_elapsed[label].quantile(0.75), cfg[eee]["75pct"]),

                checker(ok_elapsed[label].quantile(0.90), int(cfg[eee]["90pct_up"]), int(cfg[eee]["90pct_down"])),
                int(ok_elapsed[label].quantile(0.90)), cfg[eee]["90pct"],
                get_percent(ok_elapsed[label].quantile(0.90), cfg[eee]["90pct"]),

                checker(ok_elapsed[label].quantile(0.95), int(cfg[eee]["95pct_up"]), int(cfg[eee]["95pct_down"])),
                int(ok_elapsed[label].quantile(0.95)), cfg[eee]["95pct"],
                get_percent(ok_elapsed[label].quantile(0.95), cfg[eee]["95pct"]),
            )
        except:
            pass

    table_results += message_slo_requests

    from sys import platform

    if platform == "darwin":
        result_file = '{}{}'.format(args.script_name, datetime.now().strftime("_%Y-%m-%d_%H%M%S"))
    else:
        result_file = '/home/jmeter/results/{}{}'.format(args.script_name,
                                                         datetime.now().strftime("_%Y-%m-%d_%H%M%S"))

    if not args.silence:
        url = "https://discord.com/api/webhooks/1016347954258382890/F0axtrTyCVD_DlR-SmFBTE4OGbY1aOF0uyOAeMJJvhMt8qCn80rduhvnnL6ZVoYCqC4d"

        data = {
            "content": "{}".format(message_slo_requests),
            "username": "sla_checker_requests_full.py"
        }
        result = requests.post(url, json=data)

        try:
            result.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)
        else:
            print("Payload delivered successfully, code {}.".format(result.status_code))
    print(table_results)
    f = open(result_file, 'a')
    f.writelines(table_results)
    f.close()
