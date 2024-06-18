import os
from datetime import datetime

import requests
import argparse
import json

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("script_name", help="name script")
    parser.add_argument("job_url", help="url job jenkins")
    parser.add_argument("test_info", help="test info variable")
    parser.add_argument("params", help="echo parameters runs test")
    parser.add_argument("--d", help="duration test")
    args = parser.parse_args()

    out = ""
    out += ":woman_running: start {}\n**{}**\n{}".format(args.job_url, args.test_info, args.params)

    print(out)

    # url = "https://discord.com/api/webhooks/984046821317939200/MOnFHWKJEjXAVzYQ1YuEYTDENdg8tGbzxBhFpOd2rG2xx4wmzwNnL1GoKShn-OaVRlbm"
    url = "https://discord.com/api/webhooks/1016347954258382890/F0axtrTyCVD_DlR-SmFBTE4OGbY1aOF0uyOAeMJJvhMt8qCn80rduhvnnL6ZVoYCqC4d"

    data = {
        "content": "{}".format(out),
        "username": "{}".format(args.script_name)
    }
    result = requests.post(url, json=data)

    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
    else:
        print("Payload delivered successfully, code {}.".format(result.status_code))

    duration = None
    if args.d:
        duration = args.d
    elif "DURATION" in os.environ:
        duration = os.getenv('DURATION')
    # и дальше новый кусок скрипта

    dt = datetime.now()
    ts = datetime.timestamp(dt)

    file = "time.json"

    data_time_run = {
        "duration": "{}".format(duration),
        "start": {
            "datatime": "{}".format(dt),
            "timestamp": "{}".format(ts)
        }

    }

    with open("time.json", "w") as write_file:
        json.dump(data_time_run, write_file)

    print(data_time_run)
