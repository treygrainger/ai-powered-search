from aips.spark import create_view_from_collection

def load_dataframe(boosted_products_collection, boosts_collection):  
    assert(type(boosted_products_collection) == type(boosts_collection))
    create_view_from_collection(boosts_collection,
                                boosts_collection.name)    
    create_view_from_collection(boosted_products_collection,
                                boosted_products_collection.name)
    return boosts_collection.load_index_time_boosting_dataframe(boosts_collection.name, boosted_products_collection.name)