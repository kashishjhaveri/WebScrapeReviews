#!/usr/bin/env python
# coding: utf-8

# In[46]:


# Import libraries
import re
import requests
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords

import datetime
import json

import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

import nltk
from nltk.stem.porter import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer

from os import path


# In[47]:


page_counter = 1
companies = ['protect-your-home','apx_alarm','link-interactive','scout','guardian-protection-services']

# url = 'https://www.consumeraffairs.com/homeowners/'+companies[1]+'.html?page='+str(page_counter)+'#sort=top_reviews&filter=none'
base_url = 'https://www.consumeraffairs.com/homeowners/'
mid_static_url = '.html?page='
end_static_url = '#sort=top_reviews&filter=none'

ReviewsDF = pd.DataFrame(columns = ["Company","Logo","Date","Rating","Description","Keywords","Polarity_Pos","Polarity_Neu","Polarity_Neg","Polarity_Com"])
ReviewItems = []

analyser = SentimentIntensityAnalyzer()


# ## SQL Connectivity

# In[48]:


#SQL Connectivity
Server = "socialdatamier.database.windows.net"
Database = "SocialDataMiner"
Table = "SocialDataMiner.dbo.Reviews"

#Connect Server
def connectSqlServer():
    try:
        conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server} ;Server="+Server+"; Database="+Database+";Trusted_Connection=no;UID=miner;PWD=K@Mr@123")
        return conn
    except:
        print("Error in Connecting Server")
        return "" 

#Get Latest data timestamps for RSS for each website for each country
# def GetLatestTimeStamp():
#     string = "SELECT Country, Site, MAX(PublishDateTime) as PublishTimeStamp FROM " + Table + " Group By Country,Site"
#     try:
#         query = pd.read_sql_query(string, connectSqlServer())
#         return query
#     except:
#         return "Something went Wrong"
    


# In[58]:


def insert_All_Data(Reviews):
    print("Starting Insert Operation: ")
    total = len(Reviews)
    print(total)
    for index, row in Reviews.iterrows():
        print("------------")
        print(str(index+1) +"/"+str(total))
#         print(row["Title"],row["Site"],row["Country"])

        state = insert_row(row["Company"],row["Logo"],row["Date"],row["Rating"],row["Description"],row["Keywords"],row["Polarity_Pos"],row["Polarity_Neu"],row["Polarity_Neg"],row["Polarity_Com"])
        print(state)


# In[62]:


def insert_row(Company,Logo,Date,Rating,Desc,Keywords,Polarity_Pos,Polarity_Neu,Polarity_Neg,Polarity_Com):
    
#     print(Company,Logo,Date,Rating,Desc,Keywords,Polarity_Pos,Polarity_Neu,Polarity_Neg,Polarity_Com)
    
    try:
        Desc = Desc.replace("'","''")
        Keywords = Keywords.replace("'","''")
        query = "INSERT INTO [dbo].[Reviews] ([Company], [LogoUrl], [Date], [Rating], [Description], [Keywords], [Polarity_Pos],[Polarity_Neu],[Polarity_Neg],[Polarity_Com]) VALUES(\'{0}\',\'{1}\',\'{2}\',{3},\'{4}\',\'{5}\',{6},{7},{8},{9})".format(Company,Logo,Date,Rating,Desc,Keywords,Polarity_Pos,Polarity_Neu,Polarity_Neg,Polarity_Com)
        print(query)
        conn = connectSqlServer()
        cursor = conn.cursor()
        cursor.execute(query)
        # the connection is not autocommited by default. So we must commit to save our changes.
        conn.commit()
        return "Success"
    except Exception as e: 
        print(e)
        return "Fail"


# In[ ]:





# In[51]:


connectSqlServer()


# In[23]:


# Collect first page of artists’ list (Collecting and Parsing data)
page_counter = 1
companies = ['protect-your-home','apx_alarm','link-interactive','scout','guardian-protection-services']

# url = 'https://www.consumeraffairs.com/homeowners/'+companies[1]+'.html?page='+str(page_counter)+'#sort=top_reviews&filter=none'
base_url = 'https://www.consumeraffairs.com/homeowners/'
mid_static_url = '.html?page='
end_static_url = '#sort=top_reviews&filter=none'

page = requests.get(url)


# Create a BeautifulSoup object
soup = BeautifulSoup(page.text, 'html.parser')


# # Get Logo URL

# In[21]:


#Get logo url
page_body = soup.find(id='campaign_page')
logo_parent_div = page_body.find('div',class_='prf-hr__logo-box')

#Fetch logo source url
logo_url = logo_parent_div.div.img.get('src')
print(logo_url)


# # Get rating from each review

# In[22]:


#Get rating from each review
reviews_list = soup.find_all(class_='rvw js-rvw')
review = reviews_list[0]
stars = review.div.div.img['data-rating']
print(stars)


# # Get date of each review

# In[27]:


#Get date of each review
reviews_list = soup.find_all(class_='rvw js-rvw')
review = reviews_list[0]
review_body = review.find(class_='rvw-bd')
date = review_body.find(class_='ca-txt-cpt').text.replace('Original review: ','')
print('date: '+date)


# # Get the review and join all paragraphs into a single string

# In[9]:


#Get the review and join all paragraphs into a single string
review_paras = review_body.find_all('p')
review = ""
for para in review_paras:
    review = review + para.text
print(review)


# # Test run to iterate through few reviews

# In[10]:


# Pull text from all instances of <a> tag within BodyText div
# reviews_list = artist_name_list.find_all('rvw js-rvw')

reviews_list = soup.find_all(class_='rvw js-rvw')
# reviews_list = soup.find_all(class_='rvw-bd')
# artist_name_list_items = artist_name_list.find_all('a')
for review in reviews_list:
    stars = review.div.div.img['data-rating']
    print('========================================')
    print('stars: '+stars)
    
    review_body = review.find(class_='rvw-bd')
    date = review_body.find(class_='ca-txt-cpt').text.replace('Original review: ','')
    print('date: '+date)
    
    review_paras = review_body.find_all('p')
    review = ""
    for para in review_paras:
        review = review + para.text
    print(review)


# # Clean Text

# In[60]:


def clean_text(ReviewFinal):
    #Remove Image tags
    ReviewFinal = re.sub('\<img.*$', '', ReviewFinal)
    
    #Remove punctuations
    ReviewFinal = re.sub(r'[^a-zA-Z]', ' ', ReviewFinal)

    #Convert to lowercase
    ReviewFinal = ReviewFinal.lower()

    #remove tags
    ReviewFinal = re.sub(r"&lt;/?.*?&gt;"," &lt;&gt; ",ReviewFinal)

    # remove special characters and digits
    ReviewFinal = re.sub(r"(\\d|\\W)+"," ",ReviewFinal)
    
    ##Creating a list of stop words and adding custom stopwords
    stop_words = set(stopwords.words("english"))
    ##Creating a list of custom stopwords
    new_words = ["using", "show", "result", "large", "also", "iv", "one", "two", "new", "previously", "shown","people"]
    stop_words = stop_words.union(new_words)

    tokens = [t.lower() for t in ReviewFinal.split()]
    
    #Clean the unnecessary words
    clean_tokens = tokens[:]
    for token in tokens:
        if token.lower() in stop_words:
            clean_tokens.remove(token)

    return clean_tokens


# # Scrape Reviews

# In[63]:


# ReviewsDF = pd.DataFrame(columns = ["Company","Logo","Date","Rating","Description","Keywords","Polarity_Pos","Polarity_Neu","Polarity_Neg","Polarity_Com"])

ReviewsDF.drop(ReviewsDF.index, inplace=True)
company_counter = 1
total_companies = len(companies)

#Loop through each companies
for company in companies:
    
    #Loop through 10 pages to get 100 reviews for each company
    for page_counter in range(1,11):
        
        #Generate the url
        url = base_url + company + mid_static_url + str(page_counter) + end_static_url
        #Get the html page
        page = requests.get(url)

        # Create a BeautifulSoup object
        soup = BeautifulSoup(page.text, 'html.parser')
        
        #Get logo url
        page_body = soup.find(id='campaign_page')
        logo_parent_div = page_body.find('div',class_='prf-hr__logo-box')

        #Fetch logo source url
        logo_url = logo_parent_div.div.img.get('src')
#         print(logo_url)
    
    #Get all reviews for a company
        reviews_list = soup.find_all(class_='rvw js-rvw')
        total_reviews = len(reviews_list)
        index = 1
    #Iterate through each review for a company
        for review in reviews_list:
            print("Company ",str(company_counter)," / ",str(total_companies),": ",company," Page: ",str(page_counter)," / 10")
            print(str(index), " / ", str(total_reviews))
            index = index + 1
            company_counter = company_counter + 1
            #Get rating from each review
            stars = review.div.div.img['data-rating']
    #         print(stars)
            
            #Get date of each review
            review_body = review.find(class_='rvw-bd')
            date = review_body.find(class_='ca-txt-cpt').text.replace('Original review: ','')
            date = date.replace('.','')

            try:
                new_date = datetime.datetime.strptime(date, '%B %d, %Y')
            except:
#                 print(company)
#                 print('date: '+date)
                date = date.replace("Jan ","January ")
                date = date.replace("Feb ","February ")
                date = date.replace("Aug ","August ")
                date = date.replace("Sept ","September ")
                date = date.replace("Oct ","October ")
                date = date.replace("Nov ","November ")
                date = date.replace("Dec ","December ")
#                 print(date)
                new_date = datetime.datetime.strptime(date, '%B %d, %Y')   

#             print(new_date)
            
            #Get the review and join all paragraphs into a single string
            review_paras = review_body.find_all('p')
            review_desc = ""
            for para in review_paras:
                review_desc = review_desc + para.text
    
            #Clean review desc
            review_desc_clean = " ".join(clean_text(review_desc))
            #Calculate Polarity
            review_sentiment = analyser.polarity_scores(review_desc_clean)

            ReviewItem = {"Company": company,"Logo": logo_url,"Date": new_date,"Rating": stars,"Description": review_desc,"Keywords": review_desc_clean,"Polarity_Pos": review_sentiment["pos"],"Polarity_Neu": review_sentiment["neu"],"Polarity_Neg": review_sentiment["neg"],"Polarity_Com": review_sentiment["compound"] }
            ReviewItems.append(ReviewItem)

if len(ReviewItems) > 0:
    ReviewsDF = ReviewsDF.append(ReviewItems, ignore_index=True)
    ReviewItems = []
    print(len(ReviewsDF))
    insert_All_Data(ReviewsDF)
    print("Insert Completed")


# In[17]:


ReviewFinal =  clean_text(review)
print(ReviewFinal)


# In[ ]:





# In[ ]:




