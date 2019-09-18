import tweepy
import csv
import datetime
import glob
import os

####input your credentials here

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)
line_sep = ""

def retrieve_all_tweets(id):
    now = datetime.datetime.now()
    csvFile = open(id + now.strftime("%Y-%m-%d-%H-%M-%d") + '.csv', 'a')
    #Use csv Writer
    csvWriter = csv.writer(csvFile)

    for tweet in tweepy.Cursor(api.user_timeline,id=id, tweet_mode='extended').items():
        print(id + " " +  tweet.full_text)
        csvWriter.writerow([tweet.full_text])

def send_query(query, writer):
    for tweet in tweepy.Cursor(api.search, q=query,
                               lang="he",
                               since="2017-04-03",
                               tweet_mode='extended').items():
        print([tweet.full_text])
        writer.writerow([tweet.full_text])

def twits_by_hashtags(hashtags_path):
    with open(hashtags_path) as f:
        content = f.readlines()
    new_content = []
    for tag in content:
        new_content.append(tag.strip() + "#")

    now = datetime.datetime.now()
    csvFile = open("by_hashtags" + now.strftime("%Y-%m-%d-%H-%M-%d") + '.csv', 'a')
    csvWriter = csv.writer(csvFile)
    for tag in new_content:
        send_query(tag, csvWriter)



def download_labels():
    ids_to_follow = ['netanyahu', 'gantzbe', 'yairlapid', 'moshefeiglin', 'GabbayAvi', 'ishmuli',
                     'AvigdorLiberman', 'giladerdan1', 'naftalibennett', 'Ayelet__Shaked', 'Orly_levy', 'MakorRishon',
                     'AyOdeh', 'realrafiperets', 'itamarbengvir', 'tamarzandberg', 'KahlonMoshe', 'KahlonKulanu', 'kikarhashabat']
    for id in ids_to_follow:
        retrieve_all_tweets(id)


def clean_row(row):
    index = row.index('https')
    return row[0:index]



def clean_data(path):
    files = [f for f in glob.glob(path + "**/*.csv")]
    for file in files:
        new_lines = []
        with open(file, newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                if 'https://' in row[0]:
                    new_lines.append(clean_row(row[0]))
        f = open("clean_data/" + os.path.basename(file) + "_clean.csv", 'a')
        csvWriter = csv.writer(f)
        for line in new_lines:
            csvWriter.writerow([line])

def main():
    download_labels()
    twits_by_hashtags("hashtags.txt")
    clean_data("raw_data")







main()
