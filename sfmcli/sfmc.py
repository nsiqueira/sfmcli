from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from math import ceil
from multiprocessing import Pool

import requests
from prettytable import PrettyTable
from tabulate import tabulate

from sfmcli.database import initialize_database
from sfmcli.database import session
from sfmcli.models import DataExtension
from sfmcli.models import DataExtensionPage

file_handler = logging.FileHandler(filename='log.log')
stdout_handler = logging.StreamHandler(stream=sys.stdout)
handlers: list[logging.Handler] = [file_handler, stdout_handler]

log_format = '%(asctime)s:%(filename)s:%(levelname)s:%(message)s'

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=handlers,
)

logger = logging.getLogger()

TOKENS: dict[str, dict] = {}


class Pipeline:
    def __init__(self, *funcs):
        self.funcs = funcs

    def __call__(self, data, arg1, arg2):
        state = data, arg1, arg2

        for func in self.funcs:
            if state is not None:
                state = func(*state)
        return state


def get_access_token(config):
    token = TOKENS.get(config['name'])
    current_timestamp = time.time()

    if token and current_timestamp < token['expires_at_timestamp']:
        return token['access_token']

    payload = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'account_id': config['mid'],
    }
    try:
        response = requests.post(
            f"https://{config['subdomain']}.auth.marketingcloudapis.com/v2/token", data=payload,  # noqa: E501
        ).json()
        TOKENS[config['name']] = {
            'access_token': response['access_token'],
            'expires_at_timestamp': time.time() + response['expires_in'],
        }

        return response['access_token']
    except requests.exceptions.RequestException as e:
        logger.exception(
            f'access_token for {config.name}. exception {e}',
        )


def get_data_extensions_with_origin_info(config):
    data_extensions = list()
    response = requests.get(config['cloud_page_url']).json()

    for de in response:
        if session.query(DataExtension).filter_by(name=de['name'], origin_instance=config['name']).first() is None:  # noqa: E501
            data_extension = DataExtension(
                name=de['name'],
                origin_external_key=de['external_key'],
                origin_instance=config['name'],
            )
            data_extensions.append(data_extension)

    session.add_all(data_extensions)
    session.commit()


def update_data_extensions_target_info(origin, target):
    data_extensions = list()

    reponse = requests.get(target['cloud_page_url']).json()

    for de in reponse:
        data_extension = session.query(
            DataExtension,
        ).filter_by(name=de['name'], origin_instance=origin['name']).first()
        if data_extension is not None:
            data_extension.target_external_key = de['external_key']
            data_extension.target_instance = target['name']
            data_extensions.append(data_extension)

    session.add_all(data_extensions)
    session.commit()


def get_dynamic_size(record):
    size = len(str(record).encode('utf-8'))

    if size > 3000:
        return 100

    if size > 1500:
        return 500

    if len(record['values']) > 20:
        return 1000

    return 2500


def get_pages(data_extension, config):
    logger.info(f'get pages {data_extension.id}')

    base_url = f"https://{config['subdomain']}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{data_extension.origin_external_key}/rowset?$pageSize={1}"  # noqa: E501

    try:
        headers = {'Authorization': f'Bearer {get_access_token(config)}'}
        response = requests.get(base_url, headers=headers).json()

        count = response['count']

        if count > 5000000:
            logger.info(f'skipped too many records {data_extension.id}')
            return 0

        if not count > 0:
            logger.info(f'skipped no rows {data_extension.id}')
            return 0

        sample_record = response['items'][0]
        has_sfmc_key = len(sample_record['keys']) > 0
        page_size = get_dynamic_size(sample_record)

        data_extension_pages = list()

        for page in range(1, ceil(count / page_size) + 1):
            base_url = f"https://{config['subdomain']}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{data_extension.origin_external_key}/rowset?$pageSize={page_size}"

            data_extension_page = session.query(DataExtensionPage).filter_by(
                url=f'{base_url}&$page={page}',
            ).first()
            if data_extension_page is None:
                data_extension_page = DataExtensionPage(
                    url=f'{base_url}&$page={page}',
                    data_extension_id=data_extension.id,
                    status='new',
                    has_sfmc_key=has_sfmc_key,
                )
                data_extension_pages.append(data_extension_page)

        if len(data_extension_pages) > 0:
            session.add_all(data_extension_pages)
            session.commit()
    except Exception as e:
        logger.exception(
            f'get pages {data_extension.id}. exception {e}',
        )


def get_page_items_and_append_target_data(data_extension_page, origin, target):
    logger.info(f'get page items {data_extension_page.id}')

    try:
        headers = {'Authorization': f'Bearer {get_access_token(origin)}'}
        response = requests.get(data_extension_page.url, headers=headers)
        items = response.json()['items']
        items = [{**item['keys'], **item['values']} for item in items]
        return (data_extension_page, items, target)  # noqa: E501
    except Exception as e:
        session.query(DataExtensionPage).filter(DataExtensionPage.id == data_extension_page.id).update(  # noqa: E501
            {'status': 'failed'},
        )
        session.commit()
        logger.exception(
            f'get page items {data_extension_page.id}. exception {e}',  # noqa: E501
        )


def create_page_items(data_extension_page, items, target):
    logger.info(f'create items {data_extension_page.id}')

    has_sfmc_key = data_extension_page.has_sfmc_key

    try:
        headers = {'Authorization': f'Bearer {get_access_token(target)}'}
        payload = {
            'items': items,
        }

        response = {}
        if has_sfmc_key:
            response = requests.put(
                f"https://{target['subdomain']}.rest.marketingcloudapis.com/data/v1/async/dataextensions/key:{data_extension_page.data_extension.target_external_key}/rows",  # noqa: E501
                headers=headers,
                json=payload,
            ).json()
            session.query(DataExtensionPage).filter(DataExtensionPage.id == data_extension_page.id).update(  # noqa: E501
                {'request_id': response['requestId'], 'status': 'processed'},
            )
            session.commit()
            return 0

        response = requests.post(
            f"https://{target['subdomain']}.rest.marketingcloudapis.com/data/v1/async/dataextensions/key:{data_extension_page.data_extension.target_external_key}/rows",  # noqa: E501
            headers=headers,
            json=payload,
        ).json()
        session.query(DataExtensionPage).filter(DataExtensionPage.id == data_extension_page.id).update(  # noqa: E501
            {'request_id': response['requestId'], 'status': 'processed'},
        )
        session.commit()
        return 0
    except Exception as e:
        session.query(DataExtensionPage).filter(
            DataExtensionPage.id ==
            data_extension_page.id,
        ).update({'status': 'failed'})
        session.commit()
        logger.exception(
            f'create items {data_extension_page.id}. exception {e}',
        )


def clean_data_extension(data_extension, target):
    try:
        logger.info(f'clean data extension {data_extension.name}')
        if data_extension is not None:
            headers = {
                'Content-Type': 'application/soap+xml; charset=UTF-8',
            }

            body = """<?xml version="1.0" encoding="UTF-8"?>
                <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                    <s:Header>
                        <a:Action s:mustUnderstand="1">Perform</a:Action>
                        <a:To s:mustUnderstand="1">https://{subdomain}.soap.marketingcloudapis.com/Service.asmx</a:To>
                        <fueloauth xmlns="http://exacttarget.com">{access_token}</fueloauth>
                    </s:Header>
                    <s:Body xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
                        <PerformRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI" xmlns:ns2="urn:fault.partner.exacttarget.com">
                            <Action>ClearData</Action>
                            <Definitions>
                                <Definition xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="DataExtension">
                                    <CustomerKey>{external_key}</CustomerKey>
                                </Definition>
                            </Definitions>
                        </PerformRequestMsg>
                    </s:Body>
            </s:Envelope>""".format(subdomain=target['subdomain'], access_token=get_access_token(target), external_key=data_extension.target_external_key)

            response = requests.post(
                f"https://{target['subdomain']}.soap.marketingcloudapis.com/Service.asmx",
                headers=headers,
                data=body,
            )
            logger.debug(response.content)
            session.delete(data_extension)
            session.commit()
            return 0

        return 1
    except Exception as e:
        logger.exception(
            f'clean {data_extension.id}. exception {e}',
        )


def generate_report_for_data_extension(data_extension, target):
    logger.info(f'report {data_extension.name}')

    pages = list(
        session.query(DataExtensionPage).filter_by(
            data_extension_id=data_extension.id,
        ),
    )
    reports = {}

    if len(pages) == 0:
        return {}

    for page in pages:
        try:
            headers = {'Authorization': f'Bearer {get_access_token(target)}'}

            response = requests.get(
                f"https://{target['subdomain']}.rest.marketingcloudapis.com/data/v1/async/{page.request_id}/results",  # noqa: E501
                headers=headers,
            ).json()

            results = response['items']

            for index, result in enumerate(results):
                page_record_unique_errors = set()

                if result['status'] == 'Error':
                    report = reports.get(
                        f"{data_extension.id}:{result['errorCode']}",
                    )

                    if report is None:
                        report = {
                            'data_extension': data_extension.name,
                            'errors': {},
                        }

                    if result['message'] == 'Errors Occurred':

                        for error in result['errors']:
                            report_error_key = f"{error['name']}:{error['errorCode']}"
                            report_error = report['errors'].get(
                                report_error_key,
                            )

                            if report_error is None:
                                report_error = {
                                    'name': error['name'],
                                    'error_message': error['errorMessage'],
                                    'unique': 0,
                                    'count': 0,
                                }

                            page_record_error_unique_key = f'{page.id}:{index}'

                            if page_record_error_unique_key not in page_record_unique_errors:
                                report_error['unique'] = report_error['unique'] + 1
                                page_record_unique_errors.add(
                                    page_record_error_unique_key,
                                )

                            report_error['count'] = report_error['count'] + 1

                            report['errors'][report_error_key] = report_error

                    reports[f"{data_extension.id}:{result['errorCode']}"] = report

        except Exception as e:
            logger.exception(
                f'report {page.id}. exception {e}',
            )

    return reports


def populate(origin, target, update_only):
    initialize_database()
    start_time = datetime.now()
    logger.info('start of populate')

    logger.info('getting data extensions with origin info')
    get_data_extensions_with_origin_info(origin)

    logger.info('updating data extensions with target info')
    update_data_extensions_target_info(origin, target)

    with Pool() as pool:
        logger.info(
            'getting data extensions pages from origin with target info',
        )
        data_extensions = list(
            session.query(DataExtension).filter(
                DataExtension.origin_instance != None,
                DataExtension.target_instance != None,
            ),
        )

        if len(data_extensions) == 0:
            logger.info(
                'there are no data extensions',
            )
            return 0

        data_extension_tupled = []

        for data_extension in data_extensions:
            data_extension_tupled.append((data_extension, origin))

        pool.starmap(get_pages, data_extension_tupled)

        pipeline = Pipeline(
            get_page_items_and_append_target_data, create_page_items,
        )

        logger.info('this process may take a while')

        logger.info('executing pipeline')
        data_extension_pages = list(
            session.query(DataExtensionPage).filter_by(
                status='new',
            ).join(DataExtensionPage.data_extension),
        )

        if update_only:
            data_extension_pages = [
                page for page in data_extension_pages if page.has_sfmc_key
            ]

        if len(data_extension_pages) == 0:
            logger.info(
                'there are no data extension pages',
            )
            return 0

        data_extension_pages_tupled = []

        for data_extesion_page in data_extension_pages:
            data_extension_pages_tupled.append(
                (data_extesion_page, origin, target),
            )

        pool.starmap(pipeline, data_extension_pages_tupled)

    time_elapsed = datetime.now() - start_time
    logger.info(f'end of populate (hh:mm:ss.ms) {time_elapsed}')
    return 0


def clean(target):
    initialize_database()

    start_time = datetime.now()
    logger.info('start of clean')

    print(
        f'\n{tabulate([item for item in target.items()])}\n',  # noqa: E501
    )
    prompt = input(
        'do you really want to clear all data for this configuration (y/n)? ',
    )

    if not prompt == 'y':
        return 0

    data_extensions = list(
        session.query(
            DataExtension,
        ).filter_by(target_instance=target['name']),
    )

    if len(data_extensions) == 0:
        logger.info(
            'there are no data extensions to clean',
        )
        return 0

    data_extension_tupled = []
    for data_extension in data_extensions:
        data_extension_tupled.append((data_extension, target))

    with Pool() as pool:
        logger.info('cleaning all data extensions')
        logger.info('this process may take a while')
        pool.starmap(clean_data_extension, data_extension_tupled)

    time_elapsed = datetime.now() - start_time
    logger.info(f'end of clean (hh:mm:ss.ms) {time_elapsed}')
    return 0


def report(target):
    initialize_database()

    start_time = datetime.now()
    logger.info('start of report')

    data_extensions = list(
        session.query(
            DataExtension,
        ).filter_by(target_instance=target['name']),
    )

    if len(data_extensions) == 0:
        logger.info(
            'there are no data extensions to report',
        )
        return 0

    data_extension_tupled = []
    for data_extension in data_extensions:
        data_extension_tupled.append((data_extension, target))

    with Pool() as pool:
        logger.info('generating report for all data extensions')
        logger.info('this process may take a while')
        results = pool.starmap(
            generate_report_for_data_extension, data_extension_tupled,
        )

    pretty_table = PrettyTable()
    field_names = (
        'data_exension', 'error_field',
        'error_message', 'unique', 'count',
    )
    pretty_table.field_names = field_names

    for result in results:
        for report_error_code, data_extension_errors in result.items():
            for report_error_key, error in data_extension_errors['errors'].items():
                row = [
                    data_extension_errors['data_extension'], error['name'],
                    error['error_message'], error['unique'], error['count'],
                ]
                pretty_table.add_row(row)

    print(pretty_table)

    with open('report.csv', 'w', newline='') as f:
        f.write(pretty_table.get_formatted_string('csv'))

    logger.info('results are also available in the file reports.csv')

    time_elapsed = datetime.now() - start_time
    logger.info(f'end of clean (hh:mm:ss.ms) {time_elapsed}')
    return 0
