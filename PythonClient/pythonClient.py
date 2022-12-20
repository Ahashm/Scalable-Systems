import requests  # For Restapi calls
import json  # JsonConverter library
import time  # Time
import threading
# Kafka
from kafka import KafkaProducer

# Variables
ORG = "Spotify"
githubApiBaseLink = "https://api.github.com/"
# Generate your own TOKEN from: https://github.com/settings/tokens
githubToken = "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"
kafkaTopic = 'commits'

# Get list of all repositories in a given organisation
def getOrganizationRepos():
    listOfRepoNames = []
    url = f"{githubApiBaseLink}orgs/{ORG}/repos"
    response = getResponseFromApi(url)
    for i in response:
        listOfRepoNames.append(i['name'])
    newUrlsList = getAllCommitsOfRepo(listOfRepoNames)
    response = getCommitFromRepoLink(newUrlsList)

# Get all commits in a repository
def getAllCommitsOfRepo(repo):
    start = time.time()
    print("2nd call")
    #print("Getting all commits in a repository")
    repoCommits = []
    url = "{}repos/{}/{}/commits"
    response = getCommitsFromRepo(repo, url)
    for i in response:
        for each in i:
            repoCommits.append(each["url"])
    end = time.time()
    print(end - start)
    return repoCommits

# Pagination and method to retrieve all data from organization
def getResponseFromApi(url):
    start = time.time()
    print("1st call")

    #print("Getting all repositories in org")
    nextPage = True
    list = []
    while nextPage:
        response = requests.get(url, headers={'Authorization': 'token {}'.format(
            githubToken), 'User-Agent': 'App'}, params={'per_page': '100'})
        # String Converter for loading json to list
        jsonObjectsToString = json.dumps(response.json())
        currentResponse = json.loads(jsonObjectsToString)
        # Updates the list with new response
        list.extend(currentResponse)
        # Pagination, checking for more pages
        if 'next' in response.headers.get("Link"):
            url = response.links['next']['url']
        else:
            nextPage = False
    end = time.time()
    print(end - start)
    return list

def getCommitsFromRepo(repo, url):
    start = time.time()
    print("3rd call")
    list = []
    for i in repo:
        currentUrl = url.format(githubApiBaseLink, ORG, i)
        response = requests.get(currentUrl, headers={
                                'Authorization': 'token {}'.format(githubToken)})
        # String Converter for loading json to list
        jsonObjectsToString = json.dumps(response.json())
        currentResponse = json.loads(jsonObjectsToString)
        list.append(currentResponse)
    end = time.time()
    print(end - start)
    return list


def getCommitFromRepoLink(url):
    start = time.time()
    print("4th call")
    for i in url:
        response = requests.get(
            i, headers={'Authorization': 'token {}'.format(githubToken)})
        # String Converter for loading json to list
        jsonObjectsToString = json.dumps(response.json())
        currentResponse = json.loads(jsonObjectsToString)
        kafkaProducerMethod(currentResponse)
    end = time.time()
    print(end - start)


def kafkaProducerMethod(response):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092', api_version=(0, 10, 2))
    stringifyJson = json.dumps(response)
    serialized = bytes(stringifyJson, 'utf-8')
    producer.send(kafkaTopic, serialized)
    producer.flush()
    print("Kafka Producer has pushed message.")


def testApiRateLimit():
    testUrl = 'https://api.github.com/rate_limit'
    response = requests.get(
        testUrl, headers={'Authorization': 'token {}'.format(githubToken)})
    rateLimit = response.headers["X-RateLimit-Limit"]
    remainingLimit = response.headers["X-RateLimit-Remaining"]
    rateUsed = response.headers["X-RateLimit-Used"]
    return rateLimit, remainingLimit, rateUsed


def getNumberOfRepos():
    url = f'https://api.github.com/orgs/{ORG}'
    response = requests.get(
        url, headers={'Authorization': 'token {}'.format(githubToken)})
    number = response.json()["public_repos"]
    return number

while(True):
    repoAmount = int(getNumberOfRepos())
    limits = testApiRateLimit()
    if (int(limits[1]) < repoAmount):
        False
        break
    print("Rate limit is " + limits[0] + ". Currently spent " + limits[2] + ". Remaining limit is: " + limits[1] + ".")
    sleepTime = int(limits[0])/repoAmount
    getOrganizationRepos()
    time.sleep(sleepTime)
