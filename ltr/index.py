def reindex(client, index, doc_src, indexing_workers=3, indexing_batch_size=500):
    """ Reload a configuration on disk for each search engine
        (Solr a configset, Elasticsearch a json file)
        and reindex

        """
    from ltr.helpers.timed_block import timed_block

    print("Reindexing...")

    with timed_block(name='Indexing'):
        client.index_documents(index,
                               doc_src=doc_src,
                               batch_size=indexing_batch_size,
                               workers=indexing_workers)

    print('Done')
