import csv
import yaml
from glob import glob
from pathlib import Path
import argparse


def read_csv(file_path):
    data = {}
    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            endpoint = row['Label']
            data[endpoint] = {
                'Success': int(row['Success']),
                '50pct': float(row['50pct']),
                '75pct': float(row['75pct']),
                '95pct': float(row['95pct']),
                '99pct': float(row['99pct']),
            }
    return data


def read_slo(file_path):
    with open(file_path) as slo_file:
        slo = yaml.safe_load(slo_file)
    return slo


def compare_csv(csv1, csv2, tolerance_percent):
    delta_info = []
    for endpoint, metrics in csv1.items():
        if endpoint not in csv2:
            continue
        csv2_metrics = csv2[endpoint]
        for metric, value in metrics.items():
            csv2_value = csv2_metrics[metric]
            delta = csv2_value - value
            if value != 0:
                delta_percent = (delta / value) * 100
                if abs(delta_percent) > tolerance_percent:
                    delta_info.append({
                        'endpoint': endpoint,
                        'metric': metric,
                        'value': csv2_value,
                        'delta': delta,
                        'percent': delta_percent
                    })
    return delta_info


def compare_slo(csv, slo, tolerance_percent):
    default_slo = slo['default']
    endpoint_slos = slo.get('endpoints', {})
    violation_info = []
    for endpoint, metrics in csv.items():
        endpoint_slo = endpoint_slos.get(endpoint, default_slo)
        if endpoint_slo is not None:
            endpoint_tolerance_percent = endpoint_slo.get('tolerance_percent', tolerance_percent)
        else:
            endpoint_tolerance_percent = tolerance_percent
        for metric, value in metrics.items():
            if endpoint_slo is not None:
                slo_value = endpoint_slo.get(metric, default_slo[metric])
            else:
                slo_value = default_slo[metric]
            violation = value - slo_value
            if slo_value != 0:
                violation_percent = (violation / slo_value) * 100
                if abs(violation_percent) > endpoint_tolerance_percent:
                    violation_info.append({
                        'endpoint': endpoint,
                        'metric': metric,
                        'value': value,
                        'slo': slo_value,
                        'delta': violation,
                        'percent': violation_percent
                    })
    return violation_info


def save_delta_csv(delta_info, filename):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Endpoint', 'Metric', 'Value', 'Delta', 'Percent'])
        for row in delta_info:
            writer.writerow([row['endpoint'], row['metric'], row['value'], row['delta'], row['percent']])


def save_slo_csv(violation_info, filename):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Endpoint', 'Metric', 'Value', 'SLO', 'Delta', 'Percent'])
        for row in violation_info:
            writer.writerow([row['endpoint'], row['metric'], row['value'], row['slo'], row['delta'], row['percent']])


def save_delta_md(delta_info, header, filename):
    with open(filename, 'w') as f:
        f.write(f'# {header}\n\n')
        f.write('|| Endpoint || Metric || Value || Delta || Percent ||\n')
        for row in delta_info:
            f.write(
                f"|| {row['endpoint']} | {row['metric']} | {row['value']:.0f} | {row['delta']:.0f} | {row['percent']:.0f}% |\n")


def save_slo_md(violation_info, header, filename):
    with open(filename, 'w') as f:
        f.write(f'# {header}\n\n')
        f.write('|| Endpoint || Metric || Value || SLO || Delta || Percent ||\n')
        for row in violation_info:
            f.write(
                f"|| {row['endpoint']} | {row['metric']} | {row['value']:.0f} | {row['slo']:.0f} | {row['delta']:.0f} | {row['percent']:.0f}% |\n")


def sort_list_of_dicts(list_of_dicts, key):
    return sorted(list_of_dicts, key=lambda x: x[key])


def delta_grouped_by_endpoint_txt(delta_info, filename):
    with open(filename, 'w') as f:
        grouped_data = {}
        for item in delta_info:
            endpoint = item['endpoint']
            if endpoint in grouped_data:
                grouped_data[endpoint].append(item)
            else:
                grouped_data[endpoint] = [item]
        print(">>>>>>Comparison between two launches<<<<<")
        for endpoint, items in grouped_data.items():
            print(f"{endpoint}")
            f.write(f"{endpoint}\n")
            for item in items:
                print(
                    f"\t{item['metric']}: value {item['value']:.0f}, delta {item['delta']:.0f}, {item['percent']:.0f}%")
                f.write(
                    f"\t{item['metric']}: value {item['value']:.0f}, delta {item['delta']:.0f}, {item['percent']:.0f}%\n")


def slo_grouped_by_endpoint_txt(violation_info, filename):
    with open(filename, 'w') as f:
        grouped_data = {}
        for item in violation_info:
            endpoint = item['endpoint']
            if endpoint in grouped_data:
                grouped_data[endpoint].append(item)
            else:
                grouped_data[endpoint] = [item]
        print(">>>>>>SLO comparison<<<<<")
        for endpoint, items in grouped_data.items():
            print(f"{endpoint}")
            f.write(f"{endpoint}\n")
            for item in items:
                print(
                    f"\t{item['metric']}: value {item['value']:.0f}, slo {item['slo']:.0f}, delta {item['delta']:.0f}, {item['percent']:.0f}%")
                f.write(
                    f"\t{item['metric']}: value {item['value']:.0f}, slo {item['slo']:.0f}, delta {item['delta']:.0f}, {item['percent']:.0f}%\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--old_build', help="old build number")
    parser.add_argument('--new_build', help="new build number")
    parser.add_argument('--sla', help="path to sla.yml file")
    parser.add_argument('--host', help="target host")
    parser.add_argument('--simulation', help="simulation name")
    args = parser.parse_args()

    job_name = args.host + "/" + args.simulation
    path = f"/results/{job_name}"
    old_path = Path(glob(path + "/results_by_label/" + args.old_build + "_*.csv")[0])
    new_path = Path(glob(path + "/results_by_label/" + args.new_build + "_*.csv")[0])
    ## local run
    # path = f"./"
    # old_path = Path(glob(path + "/" + args.old_build + "_*.csv")[0])
    # new_path = Path(glob(path + "/" + args.new_build + "_*.csv")[0])

    csv1 = read_csv(old_path)
    csv2 = read_csv(new_path)
    slo = read_slo(args.sla)
    tolerance_percent = slo.get('tolerance_percent', 15)
    delta_info = compare_csv(csv1, csv2, tolerance_percent)
    delta_info = sort_list_of_dicts(delta_info, 'endpoint')
    if len(delta_info) > 0:
        save_delta_csv(delta_info, "comparison.csv")
        save_delta_md(delta_info, "Performance comparison", "comparison.md")
        delta_grouped_by_endpoint_txt(delta_info, "comparison.txt")
    violation_info = compare_slo(csv2, slo, tolerance_percent)
    violation_info = sort_list_of_dicts(violation_info, 'endpoint')
    if len(violation_info) > 0:
        save_slo_csv(violation_info, "slo_comparison.csv")
        save_slo_md(violation_info, "SLO comparison", "slo_comparison.md")
        slo_grouped_by_endpoint_txt(violation_info, "slo_comparison.txt")
