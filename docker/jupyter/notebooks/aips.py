import requests

def healthcheck():
  import requests    
    
  status_url = 'http://localhost:8983/solr/admin/zookeeper/statu'

  try:
    response = requests.get(status_url).json()
    if (response["responseHeader"]["status"] == 0):
      print ("Solr is up and responding.")
      print ("Zookeeper is up and responding.\n")
      print ("All Systems are ready. Happy Searching!")
  except:
      print ("Error! One or more containers are not responding.\nPlease follow the instructions in Appendix A.")

def print_status(solr_response):
  print("Status: Success" if solr_response["responseHeader"]["status"] == 0 else "Status: Failure; Response:[ " + str(solr_response) + " ]" )    

solr_url = 'http://localhost:8983/solr/'
solr_collections_api = solr_url + 'admin/collections'

def create_collection(collection_name):
    #Wipe previous collection
  wipe_collection_params = [
      ('action', "delete"), 
      ('name', collection_name)
  ]

  print("Wiping '" + collection_name + "' collection")
  response = requests.post(solr_collections_api, data=wipe_collection_params).json()
  print_status(response)

  #Create collection
  create_collection_params = [ 
      ('action', "CREATE"),
      ('name', collection_name),
      ('numShards', 1),
      ('replicationFactor', 1) ]

  print("\nCreating " + collection_name + "' collection")
  response = requests.post(solr_collections_api, data=create_collection_params).json()
  print_status(response)

def upsert_text_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(solr_url + collection_name + "/schema", json=delete_field).json()

    print("\nAdding '" + field_name + "' field to collection")
    add_field = {"add-field":{ "name":field_name, "type":"text_general", "stored":"true", "indexed":"true" }}
    response = requests.post(solr_url + collection_name + "/schema", json=add_field).json()
    print_status(response)  

def num2str(number):
  return str(round(number,4)) #round to 4 decimal places for readibility

def vec2str(vector):
  return "[" + ", ".join(map(num2str,vector)) + "]"

def tokenize(text):
  return text.replace(".","").replace(",","").lower().split()