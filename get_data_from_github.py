""" Get data from the GitHub API and stick it in a local MongoDB for querying """
import sys
import urllib2
import json
from datetime import datetime
import dateutil.parser

from pymongo import Connection
from pymongo.errors import ConnectionFailure

import keys

GITHUB_ORGANISATION = 'mozilla'

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

    # For now, drop and rebuild the entire collection everytime
    dbh.activities.remove(None, safe=True)

    # Test repository
    repo_name = 'webmaker.org'
    store_repo_actions(dbh, repo_name)


def store_repo_actions(dbh, repo_name):
    store_commits(dbh, repo_name)

def call_api(api_url):
    if api_url:
        print 'Accessing: %s' % (api_url)
        response = urllib2.urlopen(api_url)

        # Pagination on the GitHub API requires getting a 'link' value returned in the header
        # and parsing this to extract the 'next' URL
        headers = response.info().items()
        next_url = None
        link_string = None
        for item in headers:
            if item[0] == 'link':
                link_string = item[1]

        if link_string:
            links = {}
            link_headers = link_string.split(", ")
            for link_header in link_headers:
                (url, rel) = link_header.split("; ")
                url = url[1:-1]
                rel = rel[5:-1]
                links[rel] = url
            if 'next' in links:
                next_url = links['next']

        return json.load(response), next_url
    else:
        return {}, None

def store_commits(dbh, repo_name):

    more_to_fetch = True
    # initial url (will be overwritten when traversing the 'next page' links)
    api_url = 'https://api.github.com/repos/%s/%s/commits?access_token=%s&per_page=100' % (GITHUB_ORGANISATION, repo_name, GITHUB_ACCESS_TOKEN)

    while more_to_fetch:

        data, next_url = call_api(api_url)

        # traverse
        api_url = next_url

        if len(data) == 0:
            more_to_fetch = False

        # print json.dumps(data, sort_keys=True, indent=3, separators=(',', ': '))

        for event in data:
            # Commit activities have an author and a commiter.
            # We are counting both, as we are interested in the number of individual people doing the work.

            # print json.dumps(event, sort_keys=True, indent=3, separators=(',', ': '))
            # print '\n\n'

            # This validation is a bit clunky, but it does the job for now
            commit_author_date = None
            commit_author_email = None
            if event['commit']:
                if event['commit']['author']:
                    if event['commit']['author']['date']:
                        commit_author_date = event['commit']['author']['date']
                    if event['commit']['author']['email']:
                        commit_author_email = event['commit']['author']['email']

            author_login = None
            if event['author']:
                if event['author']['login']:
                    author_login = event['author']['login']

            commit_commiter_date = None
            commit_author_email = None
            if event['commit']:
                if event['commit']['committer']:
                    if event['commit']['committer']['date']:
                        commit_commiter_date = event['commit']['committer']['date']
                    if event['commit']['committer']['email']:
                        commit_committer_email = event['commit']['committer']['email']

            committer_login = None
            if event['committer']:
                if event['committer']['login']:
                    committer_login = event['committer']['login']

            # COMMIT AUTHOR
            store_single_activity(dbh,
                commit_author_date,
                repo_name,
                'commit-author',
                author_login,
                commit_author_email
                )

            # COMMIT COMMITER
            store_single_activity(dbh,
                commit_commiter_date,
                repo_name,
                'commit-committer',
                committer_login,
                commit_committer_email
                )


def cast_github_datetime(github_datetime):
    return dateutil.parser.parse(github_datetime)


def store_single_activity(dbh, github_datetime, repository, action_type, github_login, email):
    activity = { 'happened_on': cast_github_datetime(github_datetime), 'repository': repository, 'action_type': action_type, 'github_login': github_login, 'email': email }
    # print activity
    dbh.activities.insert(activity, safe=True)



if __name__ == "__main__":
    main()