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

## Combined Badges and Webmaker
# REPOS = [
#             'webmaker.org',
#             'popcorn.webmaker.org',
#             'badgekit-issue',
#             'openbadges-badgekit',
#             'popcorn-js',
#             'thimble.webmaker.org',
#             'webmaker-suite',
#             'MakeAPI',
#             'togetherjs',
#             'openbadges-directory',
#             'makeapi-client',
#             'webmaker-analytics',
#             'goggles.webmaker.org',
#             'popcorn-docs',
#             'CSOL-site',
#             'badgekit-issue-client',
#             'login.webmaker.org',
#             'openbadges-discovery',
#             'openbadger',
#             'make-valet',
#             'openbadges',
#             'webmaker-profile',
#             'eoy-fundraising',
#             'openbadges-validator-service',
#             'openbadges-validator',
#             'webmaker-download-locales',
#             'webmaker-profile-service',
#             'openbadges-specification',
#             'node-webmaker-loginapi',
#             'openbadges-bakery',
#             'badge-the-world',
#             'openbadges-badges',
#             'node-webmaker-postalservice',
#             'butter',
#             'openbadges-badgestudio',
#             'appmaker-components',
#             'badges.mozilla.org',
#             'openbadges-discussion',
#             'teach-appmaker',
#             'openbadges-cem',
#             'webliteracystandard',
#             'webmaker-ui',
#             'openbadger-client',
#             'friendlycode',
#             'badgeopolis',
#             'openbadges-backpack',
#             'webmaker-events',
#             'openbadges-bakery-service',
#             'popcornjs.org',
#             'make.mozilla.org',
#             'community.openbadges.org',
#             'events.webmaker.org',
#             'openbadges.org',
#             'webmaker-firehose',
#             'popcorn_maker'
#         ]

# # Webmaker
REPOS = [
            'webmaker.org',
            'popcorn.webmaker.org',
            'popcorn-js',
            'thimble.webmaker.org',
            'webmaker-suite',
            'MakeAPI',
            'togetherjs',
            'makeapi-client',
            'webmaker-analytics',
            'goggles.webmaker.org',
            'popcorn-docs',
            'login.webmaker.org',
            'make-valet',
            'webmaker-profile',
            'eoy-fundraising',
            'webmaker-download-locales',
            'webmaker-profile-service',
            'node-webmaker-loginapi',
            'node-webmaker-postalservice',
            'butter',
            'appmaker-components',
            'teach-appmaker',
            'webliteracystandard',
            'webmaker-ui',
            'friendlycode',
            'webmaker-events',
            'popcornjs.org',
            'make.mozilla.org',
            'events.webmaker.org',
            'webmaker-firehose',
            'popcorn_maker'
        ]

# # Badges
# REPOS = [
#             'badgekit-issue',
#             'openbadges-badgekit',
#             'openbadges-directory',
#             'CSOL-site',
#             'badgekit-issue-client',
#             'openbadges-discovery',
#             'openbadger',
#             'openbadges',
#             'openbadges-validator-service',
#             'openbadges-validator',
#             'openbadges-specification',
#             'openbadges-bakery',
#             'badge-the-world',
#             'openbadges-badges',
#             'openbadges-badgestudio',
#             'badges.mozilla.org',
#             'openbadges-discussion',
#             'openbadges-cem',
#             'openbadger-client',
#             'badgeopolis',
#             'openbadges-backpack',
#             'openbadges-bakery-service',
#             'community.openbadges.org',
#             'openbadges.org'
#         ]

_org_members = []

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

    # Work out who is likely to be staff
    update_org_members(dbh)

    # Walk through the repos and fetch the activity
    for repo_name in REPOS:
        store_repo_actions(dbh, repo_name)


def store_repo_actions(dbh, repo_name):
    store_commits(dbh, repo_name)
    store_issues(dbh, repo_name)
    store_pulls(dbh, repo_name)


def call_api(api_url):
    if api_url:
        print 'Accessing: %s' % (api_url)
        response = None
        try:
            response = urllib2.urlopen(api_url)
        except:
            print "ERROR Accessing: ", api_url
            return {}, None

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
    api_url = 'https://api.github.com/repos/%s/%s/commits?access_token=%s&per_page=100' % (GITHUB_ORGANISATION, repo_name, keys.GITHUB_ACCESS_TOKEN)

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


def store_issues(dbh, repo_name):
    # initial url (will be overwritten when traversing the 'next page' links)
    api_url = 'https://api.github.com/repos/%s/%s/issues?access_token=%s&per_page=100' % (GITHUB_ORGANISATION, repo_name, keys.GITHUB_ACCESS_TOKEN)

    more_to_fetch = True
    while more_to_fetch:

        data, next_url = call_api(api_url)
        # traverse
        api_url = next_url

        if len(data) == 0:
            more_to_fetch = False

        # print json.dumps(data, sort_keys=True, indent=3, separators=(',', ': '))
        for event in data:

            issue_date = None
            email = 'NOT-AVAILABLE'
            login = None

            # This validation is a bit clunky, but it does the job for now
            if event['created_at']:
                issue_date = event['created_at']

            if event['user']:
                if event['user']['login']:
                    login = event['user']['login']

            store_single_activity(dbh, issue_date, repo_name, 'github-issue', login, email)



def store_pulls(dbh, repo_name):
    # initial url (will be overwritten when traversing the 'next page' links)
    api_url = 'https://api.github.com/repos/%s/%s/pulls?access_token=%s&per_page=100' % (GITHUB_ORGANISATION, repo_name, keys.GITHUB_ACCESS_TOKEN)

    more_to_fetch = True
    while more_to_fetch:

        data, next_url = call_api(api_url)
        # traverse
        api_url = next_url

        if len(data) == 0:
            more_to_fetch = False


        # print json.dumps(data, sort_keys=True, indent=3, separators=(',', ': '))
        for event in data:

            issue_date = None
            email = 'NOT-AVAILABLE'
            login = None

            # This validation is a bit clunky, but it does the job for now
            if event['created_at']:
                issue_date = event['created_at']

            if event['user']:
                if event['user']['login']:
                    login = event['user']['login']

            store_single_activity(dbh, issue_date, repo_name, 'github-pull-request', login, email)


def update_org_members(dbh):
    '''Gets the list of 'members' for the github organisation, as these are likely to be staff'''

    api_url = 'https://api.github.com/orgs/%s/members?access_token=%s&per_page=100' % (GITHUB_ORGANISATION, keys.GITHUB_ACCESS_TOKEN)

    more_to_fetch = True
    while more_to_fetch:

        data, next_url = call_api(api_url)
        # traverse
        api_url = next_url

        if len(data) == 0:
            more_to_fetch = False

        for member in data:

            login = None
            # This validation is a bit clunky, but it does the job for now
            if member['login']:
                login = member['login']

            _org_members.append(login)



def cast_github_datetime(github_datetime):
    return dateutil.parser.parse(github_datetime)


def store_single_activity(dbh, github_datetime, repository, action_type, github_login, email):

    is_staff = False
    if github_login in _org_members:
        is_staff = True

    activity = { 'happened_on': cast_github_datetime(github_datetime), 'organisation': GITHUB_ORGANISATION, 'repository': repository, 'action_type': action_type, 'github_login': str(github_login), 'email': email, 'staff': is_staff }
    # print activity
    dbh.activities.insert(activity, safe=True)



if __name__ == "__main__":
    main()