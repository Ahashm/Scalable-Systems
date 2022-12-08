import requests #For Restapi calls
import json #JsonConverter library
#Kafka
from kafka import KafkaProducer

# Variables
ORG = "Spotify"
githubApiBaseLink = "https://api.github.com/"
githubToken1 = "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"
githubToken = "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"
kafkaTopic = 'Test'

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
    repoCommits = []
    url = "{}repos/{}/{}/commits"
    response = getCommitsFromRepo(repo, url)
    for i in response:
        repoCommits.append(i[0]['url'])
    return repoCommits

# Pagination and method to retrieve all data from organization
def getResponseFromApi(url):
    nextPage = True
    list = []
    while nextPage:
        response = requests.get(url, headers={'Authorization': 'token {}'.format(githubToken),'User-Agent': 'App'}, params={'per_page': '100'})
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
    return list

def getCommitsFromRepo(repo, url):
    list = []
    for i in repo:
        currentUrl = url.format(githubApiBaseLink, ORG, i)
        response = requests.get(currentUrl, headers={'Authorization': 'token {}'.format(githubToken)})
        #String Converter for loading json to list
        jsonObjectsToString = json.dumps(response.json())
        currentResponse = json.loads(jsonObjectsToString)
        list.append(currentResponse)
    return list

def getCommitFromRepoLink(url):
    for i in url:
        print("url: " + i)
        response = requests.get(i, headers={'Authorization': 'token {}'.format(githubToken)})
        #String Converter for loading json to list
        jsonObjectsToString = json.dumps(response.json())
        currentResponse = json.loads(jsonObjectsToString)
        kafkaProducerMethod(currentResponse)

def kafkaProducerMethod(response):
    producer = KafkaProducer(bootstrap_servers='kafka:9092', api_version=(0, 10, 2))
    stringifyJson = json.dumps(response)
    serialized = bytes(stringifyJson, 'utf-8')
    producer.send(kafkaTopic, serialized)
    producer.flush()
    print("Kafka Producer has pushed message.")
    
def testApiRateLimit():
    testUrl = 'https://api.github.com/rate_limit'
    response = requests.get(testUrl, headers={'Authorization': 'token {}'.format(githubToken)})
    print(response.text)

testApiRateLimit()

getOrganizationRepos()
