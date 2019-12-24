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

    

solr_url = 'http://localhost:8983/solr/'
solr_collections_api = solr_url + 'admin/collections'

def create_collection(collection_name):
    #Wipe previous collection
  wipe_collection_params = [
      ('action', "delete"), 
      ('name', collection)
  ]

  print("Wiping '" + collection + "' collection")
  response = requests.post(solr_collections_api, data=wipe_collection_params).json()
  print(response)

  #Create collection
  create_collection_params = [ 
      ('action', "CREATE"),
      ('name', collection),
      ('numShards', 1),
      ('replicationFactor', 1) ]

  print("\nCreating " + collection + "' collection")
  response = requests.post(solr_collections_api, data=create_collection_params).json()
  print(response)

def replace_text_field(collection_name, field_name):
    print("\nAdding '" + field_name + "' field to collection")
    replace_field = {"replace-field":{ "name":field_name, "type":"text_general", "stored":"true", "indexed":"true" }}
    response = requests.post(solr_url + collection_name + "/schema", json=replace_field).json()
    print(response)    