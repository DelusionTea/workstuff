import datetime

import numpy as np
import requests
import pandas
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("result_log", help="path to jtl result")
    parser.add_argument("script_name", help="name script")
    parser.add_argument("job_url", help="echo the string you use here")
    args = parser.parse_args()

    df = pandas.read_csv(args.result_log, delimiter=',')
    df_error = df['success'].loc[df['success'] == False].count()
    df_success = df['success'].loc[df['success'] == True].count()
    el = df.loc[df['success'] == True]
    new_test_time = (df["timeStamp"].max() - df["timeStamp"].min()) / 1000

    rps = df_success / ((df["timeStamp"].max() - df["timeStamp"].min()) / 1000)
    result = "-----------------\n" \
             ":recycle: {} {} \n" \
             "count {}\n" \
             "error count {}\nrps {}\n99pct {}\n95pct {}\n90pct {}\n75pct {}\n50pct {}\n25pct {}\n"
    out = result.format(
        args.script_name, args.job_url, df_success,
        str(df_error),
        int(rps),
        int(el['elapsed'].quantile(q=0.99)),
        int(el['elapsed'].quantile(q=0.95)),
        int(el['elapsed'].quantile(q=0.90)),
        int(el['elapsed'].quantile(q=0.75)),
        int(el['elapsed'].quantile(q=0.50)),
        int(el['elapsed'].quantile(q=0.25)),
    )
    print(out)

    url = "https://discord.com/api/webhooks/984046821317939200/MOnFHWKJEjXAVzYQ1YuEYTDENdg8tGbzxBhFpOd2rG2xx4wmzwNnL1GoKShn-OaVRlbm"  # webhook url, from here: https://i.imgur.com/f9XnAew.png

    data = {
        "content": "{}".format(out),
        "username": "custom username"
    }
    result = requests.post(url, json=data)

    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
    else:
        print("Payload delivered successfully, code {}.".format(result.status_code))

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
    test_time = (df.last_valid_index() - df.first_valid_index()) / 1000
    message = out
    message += "\n---Таблица времени отклика:\nlabel,success,rps,percent_request,min,pct25,pct50,pct75,pct90,pct95,pct98,pct99,max,std.dev"

    for label in labels_list:
        message += "\n{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
            label,
            int(ok_sample[label].sum()),
            int(ok_sample[label].sum() / new_test_time),
            int(round(ok_sample[label].sum() / request_count * 100, 4)),  # percentSample
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

    print(message)

    # result_file = 'results/{}{}.csv'.format(args.script_name, datetime.now().strftime("_%Y-%m-%d_%H%M%S"))
    result_file = '/home/jmeter/results/{}{}'.format(args.script_name, datetime.datetime.now().strftime("_%Y-%m-%d_%H%M%S"))

    f = open(result_file, 'a')
    f.writelines(message)
    f.close()
