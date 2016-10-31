#!/usr/bin/env python
"""
Based off of https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/clickstream/PageViewGenerator.scala
"""

import random
import argparse
import os

DEFAULT_TSV_FILE_NAME = "pageView.tsv"

PAGES = {
    "http://foo.com/": 0.7,
    "http://foo.com/news": 0.2,
    "http://foo.com/contact": 0.1
}

HTTP_STATUS = {
    200: 0.95,
    404: 0.05
}

USER_ZIP_CODE = {
    94709: 0.5,
    94117: 0.5
}

USER_ID = dict((key, 0.1) for key in range(1, 101))


class PageView:
    """
    Class to hold page view info
    """
    def __init__(self, url, status, zip_code, user_id):
        self.url = url
        self.status = status
        self.zip_code = zip_code
        self.user_id = user_id

    def __str__(self):
        return "%s\t%s\t%s\t%s\n" % (self.url, self.status, self.zip_code, self.user_id)


def pick_from_distribution(input_map):
    """
    Generates an item from a distribution map
    :param input_map: The dictionary of items to their probabilities
    :return: An item from the dictionary
    """
    rand = random.random()
    total = 0.0
    for item, prob in input_map.iteritems():
        total += prob
        if total > rand:
            return item

    # shouldn't get here if probabilities
    return random.choice(input_map.keys())


def get_next_click_event():
    """
    Generates a PageView click event
    :return: A PageView object holding the click event
    """
    user_id = pick_from_distribution(USER_ID)
    page = pick_from_distribution(PAGES)
    status = pick_from_distribution(HTTP_STATUS)
    zip_code = pick_from_distribution(USER_ZIP_CODE)
    return PageView(user_id, page, status, zip_code)


def parse_args():
    """
    Parses the args passed to the program
    :return: An args object
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_views", "-n", type=int, required=True)
    parser.add_argument("--output", "-o", default=DEFAULT_TSV_FILE_NAME)
    args = parser.parse_args()
    assert args.num_views >= 0, "Number of views must be greater than or equal to 0"
    return args


def main():
    args = parse_args()

    # ensure path exists if absolute
    if os.path.isabs(args.output):
        dirname = os.path.dirname(args.output)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    with open(args.output, 'w') as out:
        for i in range(0, args.num_views):
            page_view = get_next_click_event()
            out.write(str(page_view))


if __name__ == '__main__':
    main()

