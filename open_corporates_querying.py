#!/usr/bin/env/python
import csv
import time
from datetime import datetime
import json
import os

import psycopg2
import requests as r
from etl_configuration import SEARCH_ENDPOINT, COMPANY_COLUMNS, TEST_DATA, TARGET_DB, TARGET_TABLE_DDL, API_KEY


class QueryCompanies(object):
    # Initialise relevant endpoint(s)

    def __init__(self, term, table, debug_mode=False):
        self.search_endpoint = SEARCH_ENDPOINT
        self.term = term
        self.debug_mode = debug_mode
        # if self.debug_mode:
        #     self.target_table = "test_" + table
        # else:
        #     self.target_table = table
        self.target_table = table
        self.company_attributes = COMPANY_COLUMNS
        self.test_data = TEST_DATA
        self.filename = ""
        # The following would be called from the script's environment in case
        # the API required a key.
        self.api_key = API_KEY
        # Rate limit values and helper variables
        if debug_mode:
            self.daily_limit = 2000000
            self.monthly_limit = 5000000
        else:
            self.daily_limit = 50
            self.monthly_limit = 200
        self.target_table_ddl = TARGET_TABLE_DDL
        # Allow application to exit gracefully if connection fails.
        try:
            self.db_connection = psycopg2.connect(TARGET_DB)
        except psycopg2.Error as e:
            print(e)
        else:
            # Set autocommit to true if successful.
            self.db_connection.autocommit = True
            # Set curson.
            self.cursor = self.db_connection.cursor()

    def construct_enpoint_url(self, page=None, authentication=False):
        # Prepare term in line with api docs
        prepared_term = self.term.replace(' ', '+')

        # Construct query string and return it
        if page is None and authentication is False:
            query_string = self.search_endpoint + '?q=*' + \
                prepared_term + '*'
        elif page is not None and authentication is False:
            query_string = self.search_endpoint + '?q=*' + \
                prepared_term + '*' + '&page={}'.format(page)
        elif page is None and authentication is True:
            query_string = self.search_endpoint + '?api_token={}'.format(self.api_key) + '&q=*' + \
                prepared_term + '*'
        elif page is not None and authentication is True:
            query_string = self.search_endpoint + '?api_token={}'.format(self.api_key) + '&q=*' + \
                prepared_term + '*' + '&page={}'.format(page)

        return query_string

    def call_enpoint(self, url):
        response = r.get(url)
        if response.status_code != 200:
            print("Expected status code 200, but got the following: \n {}".format(
                response.text))
            exit()
        else:
            return response.json()

    def select_columns(self, row):
        selected_columns = list(map(row.get, self.company_attributes))
        return selected_columns

    def get_result_pagination(self):
        query_string = self.construct_enpoint_url()

        if self.debug_mode:
            pagination = 50
        else:
            pagination = self.call_enpoint(query_string)[
                "results"]["total_pages"]

        return pagination

    def extract(self):

        total_pages = self.get_result_pagination()

        if total_pages > 0:  # Just in case there are no results.
            self.filename = datetime.now().strftime(
                "%Y_%m_%d_%H_%M_%S") + "_results.csv"
            with open(self.filename, "w") as f:
                row_writer = csv.writer(
                    f, delimiter="`", quoting=csv.QUOTE_MINIMAL)
                # In terms of rate limitting by time, I'm just assuming that
                # making 50 calls is not going take longer than 24 hours and
                # similarly that 200 calls will not take longer than a month.
                max_limit_counter = 0
                min_limit_counter = 0
                for page in range(1, total_pages + 1):
                    # Each iteration makes a call to a page and then 30 calls
                    # for each company in that page for the
                    # ulitmate_beneficiary_owners.
                    calls_in_this_iteration = page + 30
                    max_limit_counter += (calls_in_this_iteration)
                    min_limit_counter += (calls_in_this_iteration)
                    if max_limit_counter > self.monthly_limit:
                        print(
                            "Monthly rate limit has been reached - please wait for 30 days.")
                        time.sleep(2592000)
                        max_limit_counter = 0
                    elif min_limit_counter > self.daily_limit:
                        print(min_limit_counter)
                        print(
                            "Daily rate limit has been reached - please wait for 24 hours.")
                        time.sleep(86400)
                        min_limit_counter = 0
                    else:
                        query_page = self.construct_enpoint_url(page=page)
                        if self.debug_mode:
                            company_results = self.test_data[
                                "results"]["companies"]
                        else:
                            company_results = self.call_enpoint(
                                query_page)["results"]["companies"]
                        for company in company_results:
                            # Occassionaly new columns will be introduced in the API
                            # which will break the pipeline. Slice response down to
                            # expected columns.
                            # NOTE: I haven't unnested any nested dictionaries so the result in the database will include
                            # JSON columns. The test did not state that the response data should be completely flattened so
                            # I'm assuming that this is acceptable.
                            sliced_company = self.select_columns(
                                company["company"])
                            # Get ultimate_beneficial_owners from seperate end
                            # point.
                            if self.debug_mode:
                                ultimate_beneficial_owners = None
                            else:
                                ubo_endpoint = "https://api.opencorporates.com/v0.4/companies/" + \
                                    sliced_company[2] + \
                                    "/" + sliced_company[1]
                                ultimate_beneficial_owners = self.call_enpoint(ubo_endpoint)["results"][
                                    "company"]["ultimate_beneficial_owners"]
                            # Update the ultimate_beneficial_owners column of
                            # the company.
                            sliced_company[
                                len(sliced_company) - 2] = ultimate_beneficial_owners
                            # self.company_attributes.append(
                            #     "ultimate_beneficial_owners")
                            # Extraction timestamp for audit purposes.
                            sliced_company[
                                len(sliced_company) - 1] = datetime.now()
                            # self.company_attributes.append("extract_tstamp")
                            row_writer.writerow(sliced_company)
                print("Data saved to csv {} succesfully.".format(self.filename))

    def load(self):
        """
        This functions follows an insert-only loading strategy. 
        So data will be loaded into the existing table 
        without truncating it first. This will provide 
        us with a dataset describing the state of a 
        queried company at different times
        """
        with open(self.filename, 'r') as data:
            try:
                self.cursor.copy_expert(
                    "COPY public.{}({}) FROM STDIN WITH CSV NULL AS '' DELIMITER AS '`'".format(self.target_table, ','.join(self.company_attributes)), data)
            except psycopg2.errors.UndefinedTable as u:
                print("public.{} does not exist in target database. Load has failed.".format(
                    self.target_table))
            except psycopg2.Error as e:
                print("Load has failed due to the following error: \n {}".format(e))
            else:
                print("Data from csv {} loaded succesfully to table public.{}.".format(
                    self.filename, self.target_table))
