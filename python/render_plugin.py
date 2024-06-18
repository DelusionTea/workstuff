import requests
import argparse

def get_all_panel_ids(api_url_base, dashboard_id, api_key):
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    URL = f"{api_url_base}/api/dashboards/uid/{dashboard_id}"
    dashboard_data = requests.get(URL, headers=headers).json()
    panel_ids = [panel['id'] for panel in dashboard_data['dashboard']['panels']]
    return panel_ids


def save_grafana_panel_as_image(api_url_base, dashboard_id, panel_id, from_time, to_time, application, api_key,
                                output_file):
    grafana_api_url = f"{api_url_base}/render/d-solo/{dashboard_id}?from={from_time}&to={to_time}&orgId=1&var-data_source=b38b7ca8-a765-4627-b445-c1c67d9f2dd6&var-application={application}&var-transaction=All&var-measurement_name=jmeter&var-send_interval=5&var-interval=1m&panelId={panel_id}&width=1000&height=500"

    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    response = requests.get(grafana_api_url, headers=headers)

    if response.status_code == 200:
        with open(output_file, 'wb') as file:
            file.write(response.content)
            print(f'График {panel_id} сохранен.')
    else:
        print(f'Ошибка сохранения графика {panel_id}: {response.text}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--application', help='Bitbucket application name')
    parser.add_argument('--from_time', default='now-3h')
    parser.add_argument('--to_time', default='now')
    parser.add_argument('--dashboard_id', default="li5vEURSz")

    args = parser.parse_args()

    # Словарь для перевода названий из Bitbucket на названия параметра application из Grafana Dashboard
    application_dict = {
        "pprb_rating": "PPRBRating",
        "riski_mob": "RiskiMobile",
        "Finmob_web": "finmobweb",
        "LTCP-241": "finmobweb" # добавлено для дебага
    }

    api_url_base = '(тут был параметр)'
    dashboard_id = args.dashboard_id
    from_time = args.from_time  # Время в формате UNIX (миллисекунды)
    to_time = args.to_time  # Время в формате UNIX (миллисекунды)
    api_key = '(тут был параметр)'
    application = application_dict[args.application]  # Переведенное название проекта из Bitbucket на Grafana
    panel_ids = get_all_panel_ids(api_url_base, dashboard_id, api_key)  # Получает через функцию все ID панелей дашборда
    # panel_ids = [224, 223] # Можно указать вручную ID панелей


for panel_id in panel_ids:
    output_file = f'OUTPUT_{panel_id}.png'
    save_grafana_panel_as_image(api_url_base, dashboard_id, panel_id, from_time, to_time, application, api_key,
                                output_file)