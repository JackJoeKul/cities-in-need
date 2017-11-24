
from pymongo import MongoClient
import copy
import glassdoor as gd
import pandas as pd

company= "uber"
def main():
    #Db settings
    client = MongoClient("mongodb://127.0.0.1:27017")
    glassdoor_db = client.glassdoor
    glassdoor_reviews = glassdoor_db.reviews

    # check if collection is empty. Drop if it isnt.
    if glassdoor_reviews.count() != 0:
        glassdoor_reviews.drop()
        glassdoor_reviews = glassdoor_db.reviews # re-create the collection

    company = "uber"
    tree = gd.company_reviews(company)

    # get all the glasssdoor reviews for this company and store in the mongodb database
    gd.get_all_reviews(tree,company,glassdoor_reviews,max_pages=257)


    # save reviews to a csv
    gd_cursor = glassdoor_reviews.find()
    gd_reviews = []

    # formatting: get pros,cons and each rating category under its own key.
    for doc in gd_cursor:
        doc['pros'] = doc['blocks'][1]
        doc['cons'] = doc['blocks'][3]
        del (doc['blocks'])
        if doc['rating_stars']:
            for rating in doc['rating_stars']:
                if rating['category'] == 'Culture & Values':
                    doc['culture/values'] = rating['rating']
                elif rating['category'] == 'Career Opportunities':
                    doc['career opportunities'] = rating['rating']
                elif rating['category'] == 'Comp & Benefits':
                    doc['compensation/benefits'] = rating['rating']
                elif rating['category'] == 'Work/Life Balance':
                    doc['work/life balance'] = rating['rating']
                elif rating['category'] == 'Senior Management':
                    doc['senior management'] = rating['rating']
        del (doc['rating_stars'])
        if doc['recomm_outlook']:
            if doc['recomm_outlook'][-1] == ' CEO':
                del doc['recomm_outlook'][-1]
        gd_reviews.append(copy.deepcopy(doc))

    # flatten the json and convert to a df
    glassdoor_df = pd.io.json.json_normalize(gd_reviews)

    # save to csv
    glassdoor_df.to_csv('glassdoor_reviews_table.csv', encoding='utf-8')



if __name__ == "__main__":
    main()