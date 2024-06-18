from datetime import datetime

import numpy as np
import pandas
import argparse
import yaml


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("result_log", help="path to jtl result")
    parser.add_argument("script_name", help="asdf")
    parser.add_argument("slo", help=" ")
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

    table_results = "label,count_request,rps,percent_request,min,25pct,50pct,75pct,90pct,95pct,98pct,99pct,max,std"

    for label in labels_list:
        try:
            table_results += "\n{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
                label,
                int(ok_sample[label].sum()),
                int(ok_sample[label].sum() / test_duration),
                int(round(ok_sample[label].sum() / request_count * 100, 4)),
                int(ok_elapsed[label].min()),
                int(ok_elapsed[label].quantile(0.25)),
                int(ok_elapsed[label].quantile(0.50)),
                int(ok_elapsed[label].quantile(0.75)),
                int(ok_elapsed[label].quantile(0.90)),
                int(ok_elapsed[label].quantile(0.95)),
                int(ok_elapsed[label].quantile(0.98)),
                int(ok_elapsed[label].quantile(0.99)),
                int(ok_elapsed[label].max()),
                int(ok_elapsed[label].std()))
        except:
            table_results += "\n{},no value".format(label)

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

    table_results += "\n\nslo check:\n"

    for label in labels_list:
        eee = get_beautiful_label(label)

        rps_label = int(ok_sample[label].sum() / test_duration)

        try:
            table_results += "\n{}:" \
                             "\n  rps: {} ({} {}%)" \
                             "\n  min: {} ({} {}%)" \
                             "\n  25pct: {} ({} {}%)" \
                             "\n  50pct: {} ({} {}%)" \
                             "\n  75pct: {} ({} {}%)" \
                             "\n  90pct: {} ({} {}%)" \
                             "\n  95pct: {} ({} {}%)" \
                             "\n  98pct: {} ({} {}%)" \
                             "\n  99pct: {} ({} {}%)" \
                             "\n  max: {} ({} {}%)".format(
                eee,
                int(rps_label), int(cfg[eee]["rps"]), get_percent(rps_label, cfg[eee]["rps"]),
                int(ok_elapsed[label].min()), int(cfg[eee]["min"]),
                ok_elapsed[label].min() / cfg[eee]["min"] * 100 - 100,
                int(ok_elapsed[label].quantile(0.25)), int(cfg[eee]["25pct"]),
                int(ok_elapsed[label].quantile(0.25) / cfg[eee]["25pct"] * 100 - 100),
                int(ok_elapsed[label].quantile(0.50)), int(cfg[eee]["50pct"]),
                int(ok_elapsed[label].quantile(0.50) / cfg[eee]["50pct"] * 100 - 100),
                int(ok_elapsed[label].quantile(0.75)), cfg[eee]["75pct"],
                int(ok_elapsed[label].quantile(0.75) / cfg[eee]["75pct"] * 100 - 100),
                int(ok_elapsed[label].quantile(0.90)), cfg[eee]["90pct"],
                int(ok_elapsed[label].quantile(0.90) / cfg[eee]["90pct"] * 100 - 100),
                int(ok_elapsed[label].quantile(0.95)), cfg[eee]["95pct"],
                int(ok_elapsed[label].quantile(0.95) / cfg[eee]["95pct"] * 100 - 100),
                int(ok_elapsed[label].quantile(0.98)), cfg[eee]["98pct"],
                int(ok_elapsed[label].quantile(0.98) / cfg[eee]["98pct"] * 100 - 100),
                int(ok_elapsed[label].quantile(0.99)), cfg[eee]["99pct"],
                int(ok_elapsed[label].quantile(0.99) / cfg[eee]["99pct"] * 100 - 100),
                int(ok_elapsed[label].max()), cfg[eee]["max"],
                int(ok_elapsed[label].max() / cfg[eee]["max"] * 100 - 100))
        except:
            pass

    result_file = '{}{}'.format(args.script_name, datetime.now().strftime("_%Y-%m-%d_%H%M%S"))
    # result_file = '/home/jmeter/results/{}/{}{}'.format(args.JOB_NAME, args.script_name, datetime.now().strftime("_%Y-%m-%d_%H%M%S"))
    print(result_file)
    print(table_results)
    f = open(result_file, 'a')
    f.writelines(table_results)
    f.close()
