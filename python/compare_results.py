import numpy as np
import pandas
import argparse
import datetime

#
# python3.9 scripts/compare_results.py ../result_9.jtl ../result_10.jtl
#
#

class ParsingJmeterCsv():
    def __init__(self, log):
        self.log = log
        self.labels_list = set()  # список всех уникальных labels
        self.labels_list_transaction = set()  # список уникальных labels транзакций
        self.labels_list_request = set()  # список уникальных labels запросов
        self.response_code_list = set()  # список уникальных кодов запросов
        self.request_error_percent = 0
        pandas.options.mode.chained_assignment = None
        self.df = pandas.read_csv(self.log, delimiter=',', low_memory=False)

        self.test_time = (self.df["timeStamp"].max() - self.df["timeStamp"].min()) / 1000
        # df_error = df['success'].loc[df['success'] == False].count()
        self.df_success = self.df['success'].loc[self.df['success'] == True].count()
        # el = df.loc[df['success'] == True] # todo: что это

        # rps = int(df_success / ((df["timeStamp"].max() - df["timeStamp"].min()) / 1000))

        self.labels_list = self.df['label'].unique()

        self.ok = self.df[self.df['success'] == True]
        self.ok['timeStamp_round'] = [round(a / 1) * 1 for a in self.ok.index]
        self.filter_request = self.ok['label'].isin(self.labels_list)
        self.ok = self.ok[self.filter_request]

        self.ok_sample = self.ok.pivot_table(
            columns=['label'], index='timeStamp_round', values='success', aggfunc=np.sum)

        self.ok_elapsed = self.ok.pivot_table(
            columns=['label'], index='timeStamp_round', values='elapsed', aggfunc=np.mean)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("old_log", help="path to jtl result")
    parser.add_argument("new_log", help="asdf")
    args = parser.parse_args()

    xxx = ParsingJmeterCsv(args.old_log)
    ddd = ParsingJmeterCsv(args.new_log)

    message = "rps"
    for label in ddd.labels_list:
        message += "\n{},{},{}".format(
            label, int(xxx.ok_sample[label].sum() / xxx.test_time), int(ddd.ok_sample[label].sum() / ddd.test_time))
    print(message)

    message = "0.25pct"
    for label in ddd.labels_list:
        message += "\n{},{},{}".format(
            label, int(xxx.ok_elapsed[label].quantile(0.25)), int(ddd.ok_elapsed[label].quantile(0.25)))
    print(message)

    message = "0.50pct"
    for label in ddd.labels_list:
        message += "\n{},{},{}".format(
            label, int(xxx.ok_elapsed[label].quantile(0.50)), int(ddd.ok_elapsed[label].quantile(0.50)))
    print(message)

    message = "0.75pct"
    for label in ddd.labels_list:
        message += "\n{},{},{}".format(
            label, int(xxx.ok_elapsed[label].quantile(0.75)), int(ddd.ok_elapsed[label].quantile(0.75)))
    print(message)

    message = "0.90pct"
    for label in ddd.labels_list:
        message += "\n{},{},{}".format(
            label, int(xxx.ok_elapsed[label].quantile(0.90)), int(ddd.ok_elapsed[label].quantile(0.90)))
    print(message)

    message = "0.95pct"
    for label in ddd.labels_list:
        message += "\n{},{},{}".format(
            label, int(xxx.ok_elapsed[label].quantile(0.95)), int(ddd.ok_elapsed[label].quantile(0.95)))
    print(message)

    message = "0.99pct"
    for label in ddd.labels_list:
        message += "\n{},{},{}".format(
            label, int(xxx.ok_elapsed[label].quantile(0.99)), int(ddd.ok_elapsed[label].quantile(0.99)))

    print(message)
