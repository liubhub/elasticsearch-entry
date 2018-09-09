import json
import requests
import logging

from time import sleep
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es

def parse(u):
    
    recipe = dict()
    ingredients = []
    
    try:
        r = requests.get(u, headers=headers)
    
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            title_section = soup.select('.recipe-summary__h1')
            submitter_section = soup.select('.submitter__name')
            description_section = soup.select('.submitter__description')
            ingredients_section = soup.select('.recipe-ingred_txt')
            calories_section = soup.select('.calorie-count')
            
            recipe['calories'] = calories_section[0].text.replace('cals', '').strip() if calories_section else 0
            recipe['description'] = description_section[0].text.strip().replace('"', '') if description_section else '-'
            recipe['submitter'] = submitter_section[0].text.strip() if submitter_section else '-'
            recipe['title'] = title_section[0].text if title_section else 0


            if ingredients_section:
                for ingredient in ingredients_section:
                    ingredient_text = ingredient.text.strip()

                    if 'Add all ingredients to list' not in ingredient_text and ingredient_text != '':
                        ingredients.append({'step': ingredient.text.strip()})
                        
            recipe['ingredients'] = ingredients


    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
        
    finally:
        return json.dumps(recipe)
    
    
    
    
def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "salads": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "submitter": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "calories": {
                        "type": "integer"
                    },
                    "ingredients": {
                        "type": "nested",
                        "properties": {
                            "step": {"type": "text"}
                        }
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created
    
def store_record(elastic_object, index_name, record):
    try:
        outcome = elastic_object.index(index=index_name, doc_type='salads', body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.ERROR)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    url = 'https://www.allrecipes.com/recipes/96/salad/'
    
    es = connect_elasticsearch()
    
    # create_index(es,'recipes')
    
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        links = soup.select('.fixed-recipe-card__h3 a')
        
        for link in links:
            sleep(2)
            result = parse(link['href'])
            print(result)
            print('Inserting to ES...')
            store_record(es,'recipes',result)
            print('='*100)