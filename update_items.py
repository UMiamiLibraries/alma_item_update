import requests
import pandas as pd
import time

apikey = '123'

def get_alma_api(barcode):

    # alma_url = f'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}/holdings/{holding_id}/items/{item_pid}/?'
    alma_url = f'https://api-na.hosted.exlibrisgroup.com/almaws/v1/items/?item_barcode={barcode}'
    print("GET: " + alma_url)
    params = {
        'apikey': apikey
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.get(alma_url, headers=headers, params=params)
    alma_record = response.json()
    # print(alma_record)
    return alma_record


def put_alma_api(alma_record, link):

    print(alma_record)
    print("PUT: " + link)
    params = {
        'apikey': apikey
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    response = requests.put(link, headers=headers, params=params, json=alma_record)
    alma_record = response.json()
    print(alma_record) 


def process_alma_data(alma_items):

    total = len(alma_items)
    for index, row in alma_items_df.iterrows():
    #for index, row in alma_items.iloc[193:].iterrows():
        #box_num = row['Box #']
        um_barcode = row['UM']
        im_barcode = row['IM']
        pieces = row['Pieces']
        location = row['location']
        note = row['Internal note 1']
        library = row['library']
        copy_id = row['Copy ID']

        if not pd.isna(um_barcode) and not pd.isna(im_barcode):
            print(f"*** {index}/{str(total)}:  {int(um_barcode)} -> {im_barcode} | {pieces} | {location} | {note} ***")
            #print(f"{index}/{str(total)}:  {int(um_barcode)} -> {im_barcode}")
            alma_record = get_alma_api(int(um_barcode))
            try:
                alma_record['item_data']['storage_location_id'] = im_barcode
                alma_record['item_data']['pieces'] = pieces
                alma_record['item_data']['location'] = {"value": location}
                alma_record['item_data']['library'] = {"value": library}
                alma_record['item_data']['internal_note_1'] = note
                alma_record['holding_data']['copy_id'] = copy_id
                put_alma_api(alma_record, alma_record['link'])
            except KeyError:
                print("no item")

            time.sleep(1)

        else:
            print(f"{index} empty; Skipping Row")


if __name__ == "__main__":
    im_file = 'files/item_file.xlsx'
    sheet = 'API'
    print("Reading items ile...")
    alma_items_df = pd.read_excel(im_file, sheet)
    alma_items_df.fillna(value="")
    print(alma_items_df.head(5))
    print(alma_items_df.columns)
    process_alma_data(alma_items_df)
