# elastic-alias-switcher

Local and remote Elasticsearch index alias switcher

### Usage

```
node index.js -h ELASTIC_HOST -p ELASTIC_PORT -u USERNAME -w PASSWORD -i ALIAS -t NEW_INDEX
```

### Parameters

```
    { name: 'host', alias: 'h', type: String, defaultValue: 'localhost' },
    { name: 'port', alias: 'p', type: String, defaultValue: '9200' },
    { name: 'user', alias: 'u', type: String },
    { name: 'pass', alias: 'w', type: String },
    { name: 'index', alias: 'i', type: String },
    { name: 'target', alias: 't', type: String }
```

All parameters are optional, except index (-i) and target (-t).

### Details

The current Elasticsearch index structure requires indexes to be aliased to a generic name in order to update data without breaking the API.

Generic index names (contracts, areas, persons, memberships, organizations) are aliases to indexes with the most recent data.

This script enables new data to be uploaded to a new index and then switching the alias to avoid downtime and enable rollback.

The process is as follows:

1.  Contracts have previously been uploaded to an index named **contracts_a**, which has an alias named **contracts**. Current data is therefore available at **contracts**.
2.  New contract data is imported and uploaded to a new index called **contracts_b**.
3.  *elastic-alias-switcher* is invoked using **contracts** as the main index to preserve (-i option) and **contracts_b** as the target collection (-t option).
4.  Updated data is now available at the same location (**contracts**) which now points to the new index.
5.  The old index (**contracts_a**) remains in place if a rollback is needed. To rollback, use the old index as the target collection.
# opensearch-tools
