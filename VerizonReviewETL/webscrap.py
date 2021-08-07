from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import time
from datetime import datetime
import os

from bs4 import BeautifulSoup
from requests import get

from hdfs.client import Client


def clickToNext(nextButtonXpath, browser):
    element = browser.find_element_by_xpath(nextButtonXpath)
    actions = ActionChains(browser)
    actions.move_to_element(element).click().perform()
    time.sleep(2)


def clickToReview(reviewButtonXpath, browser):
    reviewButton = browser.find_element_by_xpath(reviewButtonXpath)
    reviewButton.location_once_scrolled_into_view
    time.sleep(0.5)
    reviewButton.click()
    time.sleep(0.5)


def webScrapVerizon(browser, maxPage, webPageHTMLText, reviewMap, config):
    ## click to review page
    clickToReview(config["reviewButtonXpath"], browser)

    page = 1
    while (page <= maxPage):
        try:
            ## click to next page
            clickToNext(config["nextButtonXpath"], browser)

            for title in browser.find_elements_by_xpath(config['title_xpath']):
                reviewMap["Title"].append(title.text)
                reviewMap["Device"].append("Samsung Galaxy S7")

            for user_info in browser.find_elements_by_xpath(config['user_info_xpath']):
                username, submissionDate = user_info.text.split(' - ')
                reviewMap["UserNickname"].append(username)
                reviewMap["SubmissionTime"].append(submissionDate)

            for review in browser.find_elements_by_xpath(config['review_xpath']):
                reviewMap["ReviewText"].append(review.text)

            print("---------------page finish: " + str(page))
            page = page + 1
        except:
            print("-----------------Exception: failed in scrapping")
            break


    return reviewMap

def loadToHDFS(output_path, filename ,hdfsConnect, reviewDF):
    with hdfsConnect.write(os.path.join(output_path, filename), encoding='utf-8') as writer:
        reviewDF.to_csv(writer)




config = {
    "verizonUrl":'https://www.verizonwireless.com/smartphones/samsung-galaxy-s7/',
    "chromePath":'D:/Verizon/chromedriver.exe',

    "user_info_xpath":'/html/body/div[4]/div/div[5]/div[1]/div/div[1]/div/div[1]/div/div[1]/div[6]/div[1]/div/div[*]/div[1]/p/div[1]/p',
    "review_xpath":'/html/body/div[4]/div/div[5]/div[1]/div/div[1]/div/div[1]/div/div[1]/div[6]/div[1]/div/div[*]/div[1]/p/div[2]/p',
    "title_xpath":'/html/body/div[4]/div/div[5]/div[1]/div/div[1]/div/div[1]/div/div[1]/div[6]/div[1]/div/div[*]/div[1]/p/p',

    "reviewButtonXpath":'/html/body/div[4]/div/div[5]/div[1]/div/div[1]/div/div[1]/div/div[1]/div[6]/div[1]/div/div[1]/div/span/span/div/div/div/ul/li[3]/button',
    "nextButtonXpath":'/html/body/div[4]/div/div[5]/div[1]/div/div[1]/div/div[1]/div/div[1]/div[6]/div[1]/div/div[17]/nav/ul/li[6]/a'
}

hdfsCluster = "http://0.0.0.0:19000/"
hdfsNode = "hdfs://0.0.0.0:19000/"

reviewMap = {"Device": [], "Title": [],
             "ReviewText": [], "SubmissionTime": [],
            "UserNickname": []}


## load current HTML page
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
result = get(config["verizonUrl"], headers=headers)
webPageHTMLText = BeautifulSoup(result.content, "html.parser")

## load current page in chrome webdriver
browser = webdriver.Chrome(executable_path='D:/Verizon/chromedriver.exe')
browser.get(config["verizonUrl"])

## for current verizon webpage maxPage only 17
reviewScrapMap = webScrapVerizon(browser, 17, webPageHTMLText, reviewMap, config)


## connect hdfs to saving
hdfsConnect = Client(hdfsCluster)
filename = "scrapped_" + str(datetime.today())[:13].replace(" ", "-") + ".csv"
loadToHDFS(hdfsNode, filename, hdfsConnect, reviewScrapMap)

browser.quit()
