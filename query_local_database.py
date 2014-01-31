""" Get data from the GitHub API and stick it in a local MongoDB for querying """
import sys
import urllib2
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

import dateutil.parser

from pymongo import Connection
from pymongo.errors import ConnectionFailure


def main():
    # Connect to MongoDB
    try:
        c = Connection(host="localhost", port=27017)
        print "Connected successfully"
    except ConnectionFailure, e:
        sys.stderr.write("Could not connect to MongoDB: %s" % e)
        sys.exit(1)

    # Get a Database handle to a database named "github_activities"
    dbh = c["github_activities"]
    assert dbh.connection == c

    activitiescount = dbh.activities.find().count()
    print "There are %d documents in activities collection" % activitiescount

    query_date = datetime(2013,12,31)
    one_year_earlier = query_date - relativedelta(years=1)
    one_week_earlier = query_date - relativedelta(weeks=1)

    # print "one_year_earlier is %s" % str(one_year_earlier)
    # print "one_week_earlier is %s" % str(one_week_earlier)

    # activities = dbh.activities.find({"happened_on":{"$lt":query_date, "$gt":one_year_earlier}, "staff":False})
    contributors = dbh.activities.find({"happened_on":{"$lt":query_date, "$gt":one_year_earlier}, "staff":False}).distinct('github_login')

    # print "matches in query: %d" % activities.count()

    for item in contributors:
        # print "%s \t %s \t %s \t %s" % (item['repository'], item['github_login'], item['action_type'], item['staff'])
        # print str(item['happened_on'])
        print item

    print "%d unique gituhub IDs contributed, excluding Mozilla Org Members" % len(contributors)




if __name__ == "__main__":
    main()