#!/usr/bin/python3

#
# im_scraper
#
# Copyright (c) 2024 Daniel Dean <dd@danieldean.uk>.
#
# Licensed under The MIT License a copy of which you should have
# received. If not, see:
#
# http://opensource.org/licenses/MIT
#

import requests
import time
import random
import pandas as pd


def flatten_dict(my_dict):
    """ Flatten a dict.

    This will only flatten one level!

    Args:
        my_dict (dict): Dict to flatten.

    Returns:
        dict: Flattened dict.
    """
    # From (with modifications): https://stackoverflow.com/a/71952620
    result = {}
    for key, value in my_dict.items():
        if isinstance(value, dict):
            result.update({f'{key}.{k}': v for k, v in value.items()})  # Update with subdict prefixing with key
        else:
            result[key] = value  # Non-subdict elements are just copied
    return result


def fetch_results(event_id, wtc_priv_key, limit=10):
    """ Download a results set from Ironman.

    This needs the event_id and wtc_priv_key both of which you will need to find yourself. The full result set will be
    downloaded.

    Args:
        event_id (str): Event ID found from the results page.
        wtc_priv_key (str): WTC Private Key also found from the results page (should be the same for all events)
        limit (int, optional): Number of results to download per page (changing not advised). Defaults to 10.

    Returns:
        (pd.DataFrame, str): Tuple with DataFrame containing the full results set and the event name.
        
    """

    page = 0
    pages = 0
    total = 0
    data = []

    while True:

        r = requests.get(
            'https://api.competitor.com/public/result/subevent/'  + event_id,
            params={
                '$limit': limit,
                '$skip': limit * page,
                '$sort[FinishRankOverall]': 1
            },
            headers={
                'WTC_PRIV_KEY': wtc_priv_key
            }
        )

        if r.status_code != 200:
            print('Error with response.', r.json(), sep='\n')
            r.raise_for_status()

        # Set total and pages only on first response
        if not total and not pages:
            total = r.json()['total']
            pages = int(r.json()['total'] / limit)

        # Show some progress
        print(f'Downloading results... Page {page} of {pages} (Limit: {limit}, Results: {total})')

        # Extend the result set with the next response
        data.extend(r.json()['data'])

        # Increment page counter or break if over the total results
        page += 1
        if page * limit >= total:
            break

        # Sleep for random time before requesting the next page
        time.sleep(random.randint(1, 5))

    # Flatten out each result row
    for i in range(0, len(data)):
        data[i] = flatten_dict(data[i])

    return pd.DataFrame(data), data[0]['Subevent.SubEvent']
