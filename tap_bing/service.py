from bingads.service_client import ServiceClient
from bingads.authorization import AuthorizationData, OAuthDesktopMobileAuthCodeGrant, OAuthWebAuthCodeGrant
from bingads.v13.reporting import *
from concurrent.futures import ThreadPoolExecutor

import sys
import os
import os.path
import simplejson as json
import singer
from datetime import datetime

from time import gmtime, strftime
from suds import WebFault

ENVIRONMENT='production'
REPORT_FILE_FORMAT='Csv'

FILE_DIRECTORY='./'
TIMEOUT_IN_MILLISECONDS=3600000
LOGGER = singer.get_logger()

class BingReportingService:

    def __init__(self, stream, schema, config):
        LOGGER.info("Loading the web service client proxies...")
        self.config = config

        authorization_data=AuthorizationData(
            account_id=None,
            customer_id=None,
            developer_token=self.config['devToken'],
            authentication=None,
        )

        self.reporting_service_manager=ReportingServiceManager(
            authorization_data=authorization_data, 
            poll_interval_in_milliseconds=5000, 
            environment=ENVIRONMENT,
        )

        self.reporting_service=ServiceClient(
            service='ReportingService', 
            version=13,
            authorization_data=authorization_data, 
            environment=ENVIRONMENT,
        )

        self.customer_service=ServiceClient(
            service='CustomerManagementService', 
            version=13,
            authorization_data=authorization_data, 
            environment=ENVIRONMENT,
        )

        self.authenticate(authorization_data)
        self.authorization_data = authorization_data
        self.stream = stream
        self.props = [(k, v) for k, v in schema["properties"].items()]

        self.schema_map = {
            'campaign_performance_report': {
                'report_request': 'CampaignPerformanceReportRequest',
                'scope': 'AccountThroughCampaignReportScope',
                'column_type': 'CampaignPerformanceReportColumn',
                'aggregation': 'Daily',
                'account_page_size': 1000
            },
            'keyword_performance_report': {
                'report_request': 'KeywordPerformanceReportRequest',
                'scope': 'AccountThroughAdGroupReportScope',
                'column_type': 'KeywordPerformanceReportColumn',
                'aggregation': 'Daily',
                'account_page_size': 200
            },
            'ads_performance_report': {
                'report_request': 'AdDynamicTextPerformanceReportRequest',
                'scope': 'AccountThroughAdGroupReportScope',
                'column_type': 'AdDynamicTextPerformanceReportColumn',
                'aggregation': 'Daily',
                'account_page_size': 1000
            },
            'segmented_campaign_performance_report': {
                'report_request': 'CampaignPerformanceReportRequest',
                'scope': 'AccountThroughCampaignReportScope',
                'column_type': 'CampaignPerformanceReportColumn',
                'aggregation': 'Daily',
                'account_page_size': 1000
            },
            'campaign_by_device_hourly_performance_report': {
                'report_request': 'CampaignPerformanceReportRequest',
                'scope': 'AccountThroughCampaignReportScope',
                'column_type': 'CampaignPerformanceReportColumn',
                'aggregation': 'Hourly',
                'account_page_size': 1000
            },
            'search_query_performance_report': {
                'report_request': 'SearchQueryPerformanceReportRequest',
                'scope': 'AccountThroughAdGroupReportScope',
                'column_type': 'SearchQueryPerformanceReportColumn',
                'aggregation': 'Daily',
                'account_page_size': 1000
            },
            'stats_with_search_impressions_performance_report': {
                'report_request': 'CampaignPerformanceReportRequest',
                'scope': 'AccountThroughCampaignReportScope',
                'column_type': 'CampaignPerformanceReportColumn',
                'aggregation': 'Daily',
                'account_page_size': 1000
            }
        }

    def authenticate(self, authorization_data):
        self.customer_service=ServiceClient(
            service='CustomerManagementService', 
            version=13,
            authorization_data=authorization_data, 
            environment=ENVIRONMENT,
        )

        self.authenticate_with_oauth(authorization_data)

    def authenticate_with_oauth(self, authorization_data):

        authentication=OAuthWebAuthCodeGrant(
            client_id=self.config['clientId'],
            client_secret=self.config['clientSecret'],
            redirection_uri=self.config['redirectionUri'],
            env=ENVIRONMENT,
            require_live_connect=True
        )

        authorization_data.authentication=authentication

        try:
            authorization_data.authentication.request_oauth_tokens_by_refresh_token(self.config['refreshToken'])
        except OAuthTokenRequestException:
            LOGGER.info('An authentication error occured')

    def get_reports(self):
        user = self.customer_service.GetUser(
            UserId=None
        ).User
        accounts = self.search_accounts_by_user_id(self.customer_service, user.Id)
        
        acc_ids = list(map(lambda a: a['Id'], accounts))
        page_size = self.schema_map[self.stream]['account_page_size']
        pages = [acc_ids[i:i+page_size] for i in range(0, len(acc_ids), page_size)]
        with ThreadPoolExecutor(max_workers=15) as executor:
            executor.map(lambda arg: self.get_report_by_accounts_page(arg[1], page_size, arg[0]), enumerate(pages))

    def get_report_by_accounts_page(self, ids, page_size, index):
        try:
            filename = f'{self.stream}_report_{ids[0]}-{ids[-1]}_accounts_{datetime.now().strftime("%Y%m%dT%H%M%S")}.csv'
            self.get_report_for_accounts(ids, filename, index)
            file_path = f'{FILE_DIRECTORY}{filename}'
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as ex:
            LOGGER.info('An exception occured')
            LOGGER.info(ex)

    def search_accounts_by_user_id(self, customer_service, user_id):
        predicates={
            'Predicate': [
                {
                    'Field': 'UserId',
                    'Operator': 'Equals',
                    'Value': user_id,
                },
            ]
        }

        accounts=[]

        page_index = 0
        PAGE_SIZE = 1000
        found_last_page = False

        while (not found_last_page):
            paging=self.set_elements_to_none(self.customer_service.factory.create('ns5:Paging'))
            paging.Index=page_index
            paging.Size=PAGE_SIZE
            search_accounts_response = self.customer_service.SearchAccounts(
                PageInfo=paging,
                Predicates=predicates
            )

            if (page_index + 1) % 5 == 0:
                LOGGER.info(f'Retrieved accounts page #{page_index + 1}')

            if search_accounts_response is not None and hasattr(search_accounts_response, 'AdvertiserAccount'):
                accounts.extend(search_accounts_response['AdvertiserAccount'])
                found_last_page = PAGE_SIZE > len(search_accounts_response['AdvertiserAccount'])
                page_index += 1
            else:
                found_last_page=True

        return accounts

    def set_elements_to_none(self, suds_object):
        for (element) in suds_object:
            suds_object.__setitem__(element[0], None)
        return suds_object

    def get_report_for_accounts(self, account_ids, filename, index):
        try:
            report_request = self.get_report_request(account_ids)
            
            reporting_download_parameters = ReportingDownloadParameters(
                report_request=report_request,
                result_file_directory = FILE_DIRECTORY, 
                result_file_name = filename, 
                overwrite_result_file = True, # Set this value true if you want to overwrite the same file.
                timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
            )

            LOGGER.info(f'Awaiting download_report for {len(account_ids)} accounts, page {index}')
            self.download_report(reporting_download_parameters)
        except Exception as ex:
            LOGGER.info('Error while downloading report')
            LOGGER.info(ex)

    def download_report(self, reporting_download_parameters):

        report_container = self.reporting_service_manager.download_report(reporting_download_parameters)

        if(report_container == None):
            LOGGER.info("There is no report data for the submitted report request parameters.")
            return

        records = list(report_container.report_records)
        LOGGER.info(f'Retrieved {len(records)} report rows')
        for record in records:
            obj = self.map_record(record)
            singer.write_record(self.stream, obj)

        report_container.close()

    def map_record(self, record):
        obj = {}
        for prop in self.props:
            key = prop[0]
            value = ''
            if prop[1]['type'] == 'integer':
                value = record.int_value(key)
            elif key.endswith('Percent'):
                value = float(record.value(key).replace('%', '')) if record.value(key) else 0
            elif prop[1]['type'] == 'number':
                value = record.float_value(key)
            else:
                value = record.value(key)
            obj[key] = value
        return obj

    def get_report_request(self, account_ids):
        aggregation = self.schema_map[self.stream]['aggregation']
        exclude_column_headers = False
        exclude_report_footer = True
        exclude_report_header = True
        time = self.reporting_service.factory.create('ReportTime')
        date_range = self.config['dateRange']
        if ',' in date_range:
            range_parts = date_range.split(',')
            time.CustomDateRangeStart = self.parse_date(range_parts[0])
            time.CustomDateRangeEnd = self.parse_date(range_parts[1])
        else:
            time.PredefinedTime = date_range
        time.ReportTimeZone = 'PacificTimeUSCanadaTijuana'
        return_only_complete_data = False

        campaign_performance_report_request = self.get_campaign_performance_report_request(
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
            account_ids=account_ids)

        return campaign_performance_report_request

    def parse_date(self, date_str):
        date = datetime.strptime(date_str, "%Y%m%d").date()
        return {
            'Day': date.day,
            'Month': date.month,
            'Year': date.year
        }

    def get_campaign_performance_report_request(
            self,
            aggregation,
            exclude_column_headers,
            exclude_report_footer,
            exclude_report_header,
            report_file_format,
            return_only_complete_data,
            time,
            account_ids):

        report_request=self.reporting_service.factory.create(self.schema_map[self.stream]['report_request'])
        report_request.Aggregation=aggregation
        report_request.ExcludeColumnHeaders=exclude_column_headers
        report_request.ExcludeReportFooter=exclude_report_footer
        report_request.ExcludeReportHeader=exclude_report_header
        report_request.Format=report_file_format
        report_request.ReturnOnlyCompleteData=return_only_complete_data
        report_request.Time=time
        report_request.ReportName=f'{self.stream}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
        scope=self.reporting_service.factory.create(self.schema_map[self.stream]['scope'])
        scope.AccountIds={'long': account_ids }
        scope.Campaigns=None
        report_request.Scope=scope

        report_columns=self.reporting_service.factory.create(f'ArrayOf{self.schema_map[self.stream]["column_type"]}')
        schema_columns = list(map(lambda x: x[0], self.props))
        report_columns[self.schema_map[self.stream]["column_type"]].append(schema_columns)
        report_request.Columns=report_columns
        
        return report_request