import requests

# Vic notes:

ckanAPIKey = "f6325461-8ec7-4b06-99b3-cd64c014cca8"
ckanResourceID = "79adc77f-6ea7-43f9-8af0-167da76456ad"
ckanBaseURL = "https://discover.data.vic.gov.au/api/3/action/datastore_search"
api_base_url = "https://discover.data.vic.gov.au"

headers = {"authorization": ckanAPIKey}

res = requests.get(ckanBaseURL+"?resource_id="+ckanResourceID, headers=headers)

res_dicts = []

while res.status_code == 200 and len(res.json()["result"]["records"]) > 0:
	print("Records found! Appending to list...")
	res_dicts.append(res.json())
	print("Looking for more records...")
	res = requests.get(api_base_url+res.json()["result"]["_links"]["next"], headers=headers)
		
print(res_dicts)