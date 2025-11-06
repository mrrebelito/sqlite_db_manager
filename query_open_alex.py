import time
import json
import sys
import os
import math
from urllib.parse import urlencode, unquote
import requests
from sqlite_db_manager import DB

def paginate(url):
    """
    list of all of the paginated items.
    """
    items = []
    while url:
        response = requests.get(url)
        try:
            url = response.links.get("next").get("url")
        except AttributeError:
            url = None
        items.extend(response.json())
    return items


def request_api_data(doi_li):

    doi_dict_li = []
    
    for req_details in doi_li:
        # print(f"Fetching Batch {batch_number}...")
        
        try:
            # Use the 'params' dictionary for the safest request
            response = requests.get(req_details['url'])
            
            response.raise_for_status() # Check for HTTP errors


            json_data = response.json()
                        
            
            if 'results' in json_data:
                 doi_dict_li.append(json_data)
                
        except requests.exceptions.RequestException as e:
            print(f"API request for batch failed: {e}")

    return doi_dict_li


def write_json_file(json_data,f_name, dir):
    """
    write json file to directory
    """

    full_path = os.path.join(dir, f_name)

    with open(full_path, 'w', encoding="utf-8") as f:    
        json.dump(json_data, f, indent=4)

def generate_openalex_api_urls(doi_li_str, mailto) -> list[dict]:
    """
    Generates a list of API request details to batch a large list of DOIs,
    with a max of 50 DOIs per API call.

    Parameters:
        doi_li_string (str): A "str" containing ALL DOIs to be
        searched, delimited by '|'.
        mailto: email address for the OpenAlex polite pool.

    Returns:
        A list of dictionaries. Each dictionary contains:
        - 'batch_number': The 1-indexed number of this batch.
        - 'doi_count': Number of DOIs in this specific batch.
        - 'url': The fully formed, URL-encoded string (for simple use).
        - 'params': The parameters dictionary (recommended for 'requests').
    """
    base_url = "https://api.openalex.org/works"
    
    # Split all DOIs into a list of dois delimited by pipes
    doi_li = [doi.strip() for doi in doi_li_str.split('|') if doi.strip()]
    doi_total = len(doi_li)
    
    if doi_total == 0:
        return []

    # Determine number of batches needed (chunking by 50)
    batch_size = 50
    # Use math.ceil to round up (e.g., 102 DOIs -> 3 batches)
    num_batches = math.ceil(doi_total / batch_size)
    
    generated_requests = []

    # Loop through and create one request dictionary for each batch
    for i in range(num_batches):
        
        # Get the slice (chunk) of DOIs for this batch
        start_index = i * batch_size
        end_index = (i + 1) * batch_size
        doi_batch_list = doi_li[start_index:end_index]
        
        # Format the 'filter' string for this batch's DOIs
        doi_urls = [doi for doi in doi_batch_list]
        doi_filter_list = "|".join(doi_urls)
        filter_value = f"doi:{doi_filter_list}"
        
        # Define all parameters for this specific API call
        params = {
            'filter': filter_value,
            'page': 1,  # Always get page 1 of results for *this batch*
            'per-page': batch_size, # Match the input size
            'mailto': mailto
        }
        
        # Create the full URL string (as requested)
        query_string = unquote(urlencode(params))
        full_url = f"{base_url}?{query_string}"
        
        # Add the dictionary to our return list
        generated_requests.append({
            'batch_number': i + 1,
            'doi_count': len(doi_batch_list),
            'url': full_url,
            'params': params # This is the best way to use with requests
        })
            
    return generated_requests


def main(datasette_url, f_name_dict, json_dir_dict):


    # call function
    json_data = paginate(datasette_url)

    # DO NOT NEED JUST FILTER USING DATASETTE GUI
    # ADD IN FILTERED LINK
    # filter datasette data
    doi_li = [x['mods.sm_digital_object_identifier'] for x in json_data]

    if len(doi_li) == 0:
        print("no dois in JSON data")
        sys.exit(1)

    doi_li = "|".join(doi_li[0:115]) 

    # call function to handle doi list larger than 50 items
    api_urls = generate_openalex_api_urls(doi_li, "jeff.jcr04h@gmail.com")
    
    #open alex json data
    open_alex_data = request_api_data(api_urls)

    # iterate over raw open alex json 
    # to create separate parsed datasets
    parsed_main_open_alex_data = []
    
    for x in open_alex_data:
        for y in x['results']:
         
            openal_id = y.get('id')
            openal_doi = y.get('doi')
            openal_title = y.get('title')
            open_al_source = y['primary_location']['source']['display_name']
            open_al_is_oa = y['primary_location']['is_oa']

            parsed_main_open_alex_data.append({
                'id': openal_id,
                'doi': openal_doi,
                'title': openal_title,
                'source': open_al_source,
                'is_oa': open_al_is_oa
            })

    # write json file to directory
    # # json file will be read and uploaded to sqlite db    
    write_json_file(parsed_main_open_alex_data, f_name_dict['main'], json_dir_dict['main'])

    db_name = 'test.db'
    main_table_name = f_name_dict['main'].replace('.json', '')
    main_fields = {'id': str, 'doi': str, 'title': str, 'source': str, 'is_oa': str}
    main_pk = ['id']
    main_fts = ['title']


    #db_name, table_name, fields, json_path, pk, fts=None, delete_db=False
    db = DB(db_name, 'myapp')
    db.create_table(main_table_name,json_dir_dict['main'], main_fields, main_pk, main_fts)
    db.insert_json_into_table(main_table_name)

    # funding 
    parsed_author_affil_open_alex_data = []
    
    for x in open_alex_data:
        for y in x['results']:
            for z in y['authorships']:

                openal_id = y.get('id')
                openal_author = z['author']['display_name']
                openal_orcid = z['author']['orcid']

                parsed_author_affil_open_alex_data.append({
                'id': openal_id,
                'author': openal_author,
                'orcid': openal_orcid,
            })
                
    # write json file to directory
    # # json file will be read and uploaded to sqlite db    
    write_json_file(parsed_author_affil_open_alex_data,
        f_name_dict['author_affil'], json_dir_dict['author_affil'])

    author_affil_table_name = f_name_dict['author_affil'].replace('.json', '')
    author_affil_fields = {'id': str,'author':str, 'orcid':str }
    author_affil_pk = None
    author_affil_fts = ['author']

    db.create_table(author_affil_table_name,
        json_dir_dict['author_affil'], author_affil_fields, author_affil_pk, author_affil_fts)
    db.insert_json_into_table(author_affil_table_name)


if __name__ == "__main__":


    f_name_dict = {
        'main': 'main_open_alex_table.json',
        'author_affil': 'author_open_alex_table.json' 
        
    }

    json_dir_dict = {
        'main': 'data',
        'author_affil':'data'
    }


    # api endpoint for json
    # endpint should be u
    datasette_url = "http://127.0.0.1:8001/test/test_table.json?_sort=PID&mods.type_of_resource__contains=Journal&mods.sm_digital_object_identifier__notblank=1&_shape=array"
    
    main(datasette_url, f_name_dict, json_dir_dict)

