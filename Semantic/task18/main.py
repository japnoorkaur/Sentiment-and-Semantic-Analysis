
import pymongo
import math

# creating mongodb connection
connectionstring = "mongodb://dbuser:dbpass@datacluster-shard-00-00.qnva0.mongodb.net:27017,datacluster-shard-00-01.qnva0.mongodb.net:27017,datacluster-shard-00-02.qnva0.mongodb.net:27017/ReuterDb?ssl=true&replicaSet=atlas-kxawjc-shard-0&authSource=admin&retryWrites=true&w=majority"
client = pymongo.MongoClient(connectionstring)
print(client.list_database_names())
db = client.get_database("ReuterDb")
collection = db.get_collection("ReuterDbCollection")

global count
count = 0
global count1
count1 = 0
global count2
count2 = 0
global count3
count3 = 0
global count4
count4 = 0
global number
number =0
global v
v=0

class twitter_listen():
    def get_data(self):
        v = collection.estimated_document_count()   #total number of documents inn collection
        print("N=", v)
        for x in collection.find():     #iterate through documents

            text = x['Text']
            count1=self.searchKeyword(text, "Canada")
        print("Canada")
        print("df=",count1)
        x=v/count1      #calculate value
        print("N / df=",x)
        print("Log10(N/df)=",math.log(x, 10))
        print("------------")

        global count
        count=0
        for x in collection.find():

            text = x['Text']
            count4 = self.searchKeyword(text, "hot")
        print("hot")
        print("df=",count4)
        y = v / count4
        print("N /df==", y)
        print("Log10(N/df)=",math.log(y, 10))
        print("------------")

        count = 0
        for x in collection.find():

            text = x['Text']
            count2 = self.searchKeyword(text, "cold")
        print("cold")
        print("df=",count2)
        z = v / count2
        print("N /df=", z)
        print("Log10(N/df)=",math.log(z, 10))
        print("------------")
        count = 0
        for x in collection.find():

            text = x['Text']
            count3 = self.searchKeyword(text, "rain")
        print("rain")
        print("df=",count3)
        w = v / count3
        print("N /df=", w)
        print("Log10(N/df)=",math.log(w, 10))
        print("------------")
        count = 0


    def countFrequency(self):
        doc=0
        store={}
        noOfWords=0
        frequency=0
        for x in collection.find():     #for 1 document
            text = x['Text']
            doc+=1
            words = text.split()
            for word in words:
                noOfWords+=1        #total words in a document
                if word=='Canada':
                    global number
                    number += 1     #total frequency in a document

            if 'Canada' in text:
                frequency = number / noOfWords
                store[doc] = frequency
                print("Article ",doc)
                print("Total word=",noOfWords)
                print("frequency=",number)
                print("------------")

            number=0
            noOfWords=0

        max_value = max(store. values())

        for key, value in store.items():
            if value == max_value:
                print("key=",key)
                self.printarticle(key)
                break

    def printarticle(self,key):
        doct=0
        for x in collection.find():  # for 1 document
            doct += 1
            text = x['Text']
            if doct==key:
                print("News with highest relative frequency is=")
                print(text)     #print highest relative frequency news

    def searchKeyword(self, dataToSearch,Keyword):
        if Keyword in dataToSearch:
            global count
            count+=1
        return count



if __name__ == '__main__':
    t=twitter_listen()
    t.get_data()
    t.countFrequency()

