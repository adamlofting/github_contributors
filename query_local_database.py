""" Get data from the GitHub API and stick it in a local MongoDB for querying """
import sys
import urllib2
import json
from datetime import datetime
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




if __name__ == "__main__":
    main()