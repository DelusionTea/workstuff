import pandas
import logging
import argparse
import math
from decimal import Decimal, ROUND_HALF_EVEN

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prev_max", default="1937807", help="tph of prev test")
    parser.add_argument("--profile", default="profile.csv", help="path to 100 csv")
    parser.add_argument("--steps", default="10", help="steps count")
    parser.add_argument(
        "--first_step", default="1", help="first step percent 100 as default"
    )

    args = parser.parse_args()

    logging.basicConfig(
        filename="Profile_result.csv",
        format="%(filename)s[LINE:%(lineno]d# %(levelname)-8s [%(asctime)s] %(message)s",
        level=logging.DEBUG,
    )

    df = pandas.read_csv(args.profile, delimiter=",")
    profile = df.to_dict(orient="records")

    sumtph = 0
    for row in profile:
        sumtph += int((row["tph"]))
    print("sumtph: " + str(sumtph))
    if sumtph < int(args.prev_max) and (sumtph * 2.8) < int(args.prev_max):
        step = Decimal(str(sumtph / (int(args.prev_max) - sumtph))).quantize(
            Decimal("0.1"), ROUND_HALF_EVEN
        )
    else:
        step = 0.2
    print("step: " + str(step))

    result_dict = {}
    rounded_profile = {}

    listofrows = [d["script"] for d in profile]
    listofrows.append("sum")
    listofrows.append("tps")
    result_dict["scripts"] = listofrows

    roundlist = []
    sumround = 0
    for row in profile:
        value = sumtph * (math.ceil(row["tph"] / sumtph * 100)) / 100
        roundlist.append(int(value))
        sumround += int(value)

    percentlist = []
    checkpercent = 0
    for row in roundlist:
        value = str(
            (
                Decimal(str((row / sumround) * 100)).quantize(
                    Decimal("1"), ROUND_HALF_EVEN
                )
            )
        )
        percentlist.append(value + "%")
        checkpercent += int(value)
    percentlist.append(str(checkpercent) + "%")
    percentlist.append("-")
    result_dict["percentage"] = percentlist

    for count in range(int(args.steps)):

        current_step = int(args.first_step) + ((count) * step)
        list = []
        sum = 0
        for row in roundlist:
            list.append(int((int(row) * current_step)))
            sum += int((int(row) * current_step))
        list.append(int(sum))
        list.append(
            (Decimal(str(sum / 3600)).quantize(Decimal("0.1"), ROUND_HALF_EVEN))
        )
        result_dict[str(int(current_step * 100)) + "%"] = list

    df = pandas.DataFrame(result_dict)
    df.to_csv("Profile_result.csv", index=False)