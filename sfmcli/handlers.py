from __future__ import annotations

import json
from os.path import exists

from tabulate import tabulate

from sfmcli.sfmc import clean
from sfmcli.sfmc import populate
from sfmcli.sfmc import report

CONFIG_FILE = 'config.json'


def save_config(config) -> int:
    with open('config.json', 'w') as f:
        json.dump(config, f)

    return 0


def load_config() -> dict:
    config: dict = {}

    if exists(CONFIG_FILE):
        with open('config.json') as f:
            config = json.load(f)

    return config


def config_handler(args) -> int:

    action: str = args.action
    config = load_config()

    if action == 'new':
        ready: bool = False
        temp_config: dict = {}
        while not ready:
            name: str = input('name of your environment: ')

            if config.get(name) is not None:
                print('a environment with this name already exists')
                return 1

            client_id: str = input('client id of your environment : ')
            client_secret: str = input('client secret of your environment: ')
            subdomain: str = input('subdomain of your environment: ')
            mid: str = input('mid of your environment: ')
            cloud_page_url: str = input('cloud page url of your environment: ')

            temp_config = {
                'name': name,
                'client_id': client_id,
                'client_secret': client_secret,
                'subdomain': subdomain,
                'mid': mid,
                'cloud_page_url': cloud_page_url,
            }

            print(
                f"\n{tabulate(temp_config.items(),headers=['option', 'value'])}\n",  # noqa: E501
            )
            confirmation = input(
                'this environment values looks good for you (y/n)? ',
            )
            ready = confirmation == ('y')

        config[temp_config['name']] = temp_config

        save_config(config)
        print(f"{temp_config['name']} environment saved")

        return 0

    if action == 'list':
        print('available environments')
        if len(config.keys()) > 0:
            configs = config.values()

            for config in configs:
                print(
                    f'\n{tabulate([item for item in config.items()])}\n',  # noqa: E501
                )
            return 0

        print('\nno environments\n')
        return 0

    if action == 'remove':
        if len(config.keys()) > 0:
            name = input('name of the environment you want to remove: ')
            if config.get(name) is None:
                print('provide a valid environment name')
                return 1

            config.pop(name)
            save_config(config)
            print(f'{name} environment removed')
            return 0
        print('\nno environments\n')
        return 0
    return 1


def populate_handler(args):
    config = load_config()

    origin = config.get(args.origin)

    if origin is None:
        print('provide a valid origin environment name')
        return 1

    target = config.get(args.target)

    if target is None:
        print('provide a valid target environment name')
        return 1

    populate(origin, target, args.update_only)

    return 0


def clean_handler(args):
    config = load_config()

    target = config.get(args.target)

    if target is None:
        print('provide a valid target environment name')
        return 1

    clean(target)

    return 0


def report_handler(args):
    config = load_config()

    target = config.get(args.target)

    if target is None:
        print('provide a valid target environment name')
        return 1

    report(target)

    return 0
