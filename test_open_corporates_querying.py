import unittest
import os
from datetime import datetime
import psycopg2
from open_corporates_querying import QueryCompanies


class TestQueryCompanies(unittest.TestCase):
    """Tests for open_corporates_querying.py"""

    def setUp(self):
        # Set up test
        self.test_query = QueryCompanies("barclays bank", "company")
        # print(self.test_query.search_endpoint)
        self.test_query.api_key = "XXX"
        self.search_endpoint = "https://api.opencorporates.com/v0.4/companies/gb/09026697"
        # self.test_query.filename = datetime.now().strftime(
        #     "/%Y_%m_%d_%*") + "_results.csv"
        self.test_query.test_target_table = "test_" + self.test_query.target_table

    def test_construct_enpoint_url(self):
        """

        Test case 1
        ___________
        Arg 1 = barclays bank
        Arg 2 = 1
        Arg 3 = False
        Output = https://api.opencorporates.com/v0.4/companies/search?q=barclays+bank&page=1


        """

        # Run tests
        self.assertEqual(self.test_query.construct_enpoint_url(
            None, False), "https://api.opencorporates.com/v0.4/companies/search?q=*barclays+bank*")
        self.assertEqual(self.test_query.construct_enpoint_url(
            None, True), "https://api.opencorporates.com/v0.4/companies/search?api_token=XXX&q=*barclays+bank*")
        self.assertEqual(self.test_query.construct_enpoint_url(
            1, False), "https://api.opencorporates.com/v0.4/companies/search?q=*barclays+bank*&page=1")
        self.assertEqual(self.test_query.construct_enpoint_url(
            1, True), "https://api.opencorporates.com/v0.4/companies/search?api_token=XXX&q=*barclays+bank*&page=1")

    def test_call_endpoint(self):
        """Assumes rate limit has not been reached."""
        self.assertEqual(self.test_query.call_enpoint(self.search_endpoint)["results"][
            "company"]["company_number"], "09026697")

    def test_select_columns(self):
        """Tests to see whether function returns correct number of columns."""
        test_row = {'name': '! ! ! 1ST CHOICE ANDROID SMART-PHONE TUTORING, INC.', 'company_number': 'C3517133', 'jurisdiction_code': 'us_ca', 'incorporation_date': '2012-11-02', 'dissolution_date': None, 'company_type': 'DOMESTIC STOCK', 'registry_url': 'https://businessfilings.sos.ca.gov/frmDetail.asp?CorpID=03517133', 'branch': None, 'branch_status': None, 'inactive': True, 'current_status': 'Dissolved', 'created_at': '2012-11-10T03:15:55+00:00', 'updated_at': '2019-12-03T12:53:16+00:00', 'retrieved_at': '2019-11-28T01:23:56+00:00', 'opencorporates_url':
                    'https://opencorporates.com/companies/us_ca/C3517133', 'previous_names': [], 'source': {'publisher': 'California Secretary of State', 'url': 'https://businessfilings.sos.ca.gov/frmDetail.asp?CorpID=03517133', 'retrieved_at': '2019-11-28T01:23:56+00:00'}, 'registered_address': {'street_address': '420 N MCKINLEY ST #111-182\nCORONA CA 92879', 'locality': None, 'region': None, 'postal_code': None, 'country': 'United States'}, 'registered_address_in_full': '420 N MCKINLEY ST #111-182\nCORONA CA 92879', 'industry_codes': [], 'restricted_for_marketing': None, 'native_company_number': None}
        number_of_columns = len(self.test_query.select_columns(test_row))
        self.assertEqual(number_of_columns, 24)

    def test_get_result_pagination(self):
        self.assertEqual(self.test_query.get_result_pagination(), 5)

    def test_pipeline(self):
        """This tests both extract() and load()."""
        self.test_query.extract()
        self.assertTrue(os.path.exists(self.test_query.filename))
        # Create a test version of the target table.
        statement = """CREATE TABLE IF NOT EXISTS public.{0} (
                                  like public.{1} including all);
                        TRUNCATE public.{0}""".format(
            self.test_query.test_target_table, self.test_query.target_table)
        try:
            self.test_query.cursor.execute(statement)
        except psycopg2.Error as e:
            print(e)
            exit()
        else:
                # A bit messy but need to swap these values in order for load()
                # to load the test table.
            self.test_query.target_table = self.test_query.test_target_table
            self.test_query.load()
            self.test_query.cursor.execute(
                "SELECT COUNT(*) FROM public.{}".format(self.test_query.test_target_table))
            target_row_count = self.test_query.cursor.fetchall()[0]
            # There should be a row count of greater than 0.
            self.assertNotEqual(target_row_count, 0)

    # def run_tests(self):
    #     """
    #     This follows

    #     """
    #     # print(dir(self.test_query))
    #     self.test_construct_enpoint_url()
    #     # self.test_call_endpoint_url()
    #     self.test_select_columns()
    #     self.test_get_result_pagination()

if __name__ == '__main__':
    unittest.main()
    # test_suite = TestQueryCompanies()
    # test_suite.run_tests()
