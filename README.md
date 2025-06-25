# Summary

_Automated unit tests_:

Tests are included in `test_open_corporates_querying.py`. Please see `Usage` section below for how to run the tests.

_Error handling if the API returns error codes_:

Lines 70 to 77 of `open_corporates_querying.py` show my implementation of this:
```python
    def call_enpoint(self, url):
        response = r.get(url)
        if response.status_code != 200:
            print("Expected status code 200, but got the following: \n {}".format(
                response.text))
            exit()
        else:
            return response.json()
```
I have also included error handling for other situations where appropriate.

_Handling of API rate limiting_:

As the time limits are quite long (the lowest limit is a day) I took the simple approach of simply counting the calls I made. You can see my implementation between lines 107 and 126 of `open_corporates_querying.py`.
```python
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
```
An improvement on this would be to place the functionality in a decorator function.
Note: as the minimum rate limit period is a day and the rate is not enough to traverse all the pages which the term 'smart' outputs, I recommend running the script in debug mode (instructions in `Usage` below). This will allow you to observe the whole functionality.

_How to handle API keys/secrets if the API were to require such things_:

I call a pretend API key environment variable in the configuration file `etl_configuration.py` 
which is then imported into `open_corporates_querying.py`, in order to construct the query string url.
This way the API key is entered only once in the environment in which it's used, so multiple scripts can
potentially use the one variable. It also prevents the risk of mistakenly pushing the API key into a public 
repository by keeping it inline.
An innovation on this method might be to use something like [AWS AppConfig](https://aws.amazon.com/systems-manager/features/), in order to store secrets independently of particular resources. 

_Consideration for how the code would perform with much higher volumes of data (e.g. 100,000 company records)_:

I normally use pandas in order to create and manipulate csv files, however it requires quite a lot of optimization
in order to be performant with large datasets. That is why I used the Standard Library's csv module, which is more 
lean and this allows for faster computation of multiple lines.


## Installation

_This assumes a unix environment and that you are using python 3.*_

Clone this repository:
```shell
$ git clone https://github.com/MihalisW/open_corporates_querying.git
```

Create virtualenv:
```shell
$ python3 -m venv /path/to/new/virtual/environment
```

Activate virtuaenv:
```shell
$ source /path/to/new/virtual/environment/bin/activate
``` 

Go into directory you just created:
```shell
$ cd environment
``` 

Make sure you update pip:
```shell
$ pip3 install --upgrade pip
```

Install required packages from requirements.txt file:
```shell
$ pip3 install -r requirements.txt
```

## Usage

_The database I used was postgresql 11.0_

Use by running main.py and passing in the term you are searching (in this case 'smart') and the name of the target table (in this case 'company'):
```shell
$ python3 main.py smart company
```
I recommend using it in debug mode due to the rate limiting, like so:
```shell
$ python3 main.py smart company True
```
Unit tests are included and can be run like so:
```shell
$ python3 -m unittest test_open_corporates_querying.py
```
