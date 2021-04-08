from pymed import PubMed
import pymongo
import spacy
from time import time


topic_1 = "migraine"
topic_2 = "smoking"
causation_keywords = ["caused", "due", "relationship", "association", "associated"]  # cannot be empty
db = 0
max_results = 2000000


def connect_mongodb():  # Connect to MongoDB
    try:
        client = pymongo.MongoClient('mongodb://{}:{}'.format('localhost', '27017'))
        global db
        db = client['pubmed']
    except RuntimeError:
        exit("Could not connect to mongoDB!\nExiting program...")

    # instantiate collection variable to read topic 1 collection from mongoDB
    global collec
    collec = db[topic_1]

    # instantiate collection variable to write to mongoDB
    global collec_out
    collec_out = db["{} & {} analyzed".format(topic_1, topic_2)]
    return


def setup_spacy():
    global nlp
    nlp = spacy.load("en_core_web_sm")
    nlp.add_pipe("merge_entities")  # combine known entities
    nlp.add_pipe("merge_noun_chunks")  # combine syntactically related noun objects
    return


def fetch_articles():
    added = 0
    for topic in [topic_1, topic_2]:

        # instantiate mongoDB collection object
        topic_collec = db[topic]

        # building search query (2011 - present), topic found in TITLE, ABSTRACT or KEYWORDS
        search_term = "(((\"2011\"[Date - Publication] : \"3000\"[Date - Publication])) AND (({}[Title/Abstract]) OR" \
                      " ({}[MeSH Major Topic]) OR ({}[MeSH Terms]) AND (".format(topic, topic, topic)
        for word in causation_keywords:  # complete build by appending causation links and trimming
            search_term += "(" + word + "[Title/Abstract]) OR "
        search_term = search_term[:-4] + ")"

        # NIH PubMed API call
        pubmed = PubMed(tool="PubMedSearcher", email="myemail@ccc.com")
        results = pubmed.query(search_term, max_results=max_results)
        print("\n<-------------------------------------->\n\n FETCHING \"{}\"".format(topic))

        # take necessary parts from results and store on mongoBD
        for paper in results:

            # use provided toDict() function to make things easy
            article = paper.toDict()

            # trim article keeping only relevant fields
            trimmed = {
                u'pubmed_id': article['pubmed_id'].partition('\n')[0],
                u'title': article['title'],
                u'keywords': article.get('keywords', []),
                # u'journal':article['journal'],
                u'abstract': article['abstract'],
                u'conclusions': article.get('conclusions', ""),
                # u'methods':article['methods'],
                u'results': article.get('results', ""),
                # u'copyrights':article['copyrights'],
                # u'doi':article['doi'],
                u'publication_date': str(article['publication_date']),
                # u'authors':article['authors']
            }

            topic_collec.insert_one(trimmed)
            added += 1
            # check in with user
            if added % 500 == 0:
                print("Added {} docs in ".format(added) + str(int(time() - start)) + " seconds")

        print("\n\n*** {} FETCHED ***\n".format(topic))
    print("{} documents retrieved and stored in ".format(added) + str(int(time() - start)) + " seconds\n")


def nlp_analysis(text):
    connection = 0  # score for connectedness
    analyzed = nlp(text)
    for sentence in analyzed.sents:
        for token in sentence:

            # efficient way to search for causation keyword
            if token.pos_ == "VERB" and token.text in causation_keywords:
                connection += 1

                # look earlier in the sentence for topic
                possible_subj1 = [w for w in token.head.lefts]
                for noun_phrase in possible_subj1:

                    # check if either topic is present and connected to a causation keyword
                    if topic_1 in noun_phrase.text.split() or topic_2 in noun_phrase.text.split():
                        connection += 10

                        # manual parse to search for presence of second topic anywhere in the same sentence
                        if topic_1 in sentence.text.split() and topic_2 in sentence.text.split():
                            connection += 100
    return connection


def score_links():
    global doc
    for doc in collec.find({}, {"_id": 0}):
        connection_score = 0

        # check for topic match in title
        if doc["title"].lower().find(topic_2) != -1:

            # check abstract section is not empty before analyzing
            if doc["abstract"] is not None:
                connection_score += nlp_analysis(doc["abstract"].lower())
            # analyze title as well
            doc["connection"] = nlp_analysis(doc["title"].lower()) + connection_score
            # insert results into output collection
            collec_out.insert_one(doc)

        # check if keywords list is not empty before checking for keyword match
        elif doc["keywords"]:
            for keyword in doc["keywords"]:
                if str(keyword).lower().find(topic_2) != -1:

                    # check abstract section is not empty before analyzing
                    if doc["abstract"] is not None:
                        connection_score += nlp_analysis(doc["abstract"].lower())
                    # analyze title as well
                    doc["connection"] = nlp_analysis(doc["title"].lower()) + connection_score
                    # insert results into output collection
                    collec_out.insert_one(doc)
                    break
    return


if __name__ == "__main__":
    connect_mongodb()
    setup_spacy()
    start = time()
    fetch_articles()
    score_links()
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("\n\n\n\n ****** \"{} & {} analyzed\" collection built in {} seconds ******\
          ".format(topic_1, topic_2, int(time() - start)))
    sorted = collec_out.find().sort("connection", -1)
    for doc in sorted[:10]:
        print("\nConnection score {}".format(doc["connection"]))
        print("Title: {}\n".format(doc["title"]))

