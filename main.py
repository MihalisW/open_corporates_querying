#!/usr/bin/env/python
from open_corporates_querying import QueryCompanies
import sys

if __name__ == '__main__':

    def check_for_parameter(position):
        """
        Helper function to check whether the required paramenters (term, target_table) have been passed.

        """
        try:
            terminal_argument = sys.argv[position]
        except IndexError as i:
            if position == 1:
                print("Please provide a term to search for as the first argument.")
            elif position == 2:
                print(
                    "Please provide a target table to load into as the second argument.")
            elif position == 3:
                return False  # Debug mode is False by default.
            elif position > 3:
                print("Unknown argument.")
            # Exit execution as we do not have all the required parameters.
            exit()
        else:
            return terminal_argument

    term = check_for_parameter(1)

    target_table = check_for_parameter(2)

    debug_mode = check_for_parameter(3)

    query = QueryCompanies(term, target_table, debug_mode)

    # Extract
    query.extract()

    # Load
    query.load()
