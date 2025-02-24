#!/usr/bin/env node
'use strict';
const { Client } = require("@opensearch-project/opensearch");
const commandLineArgs = require('command-line-args');

const optionDefinitions = [
    { name: 'uri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Opensearch URI
    { name: 'index', alias: 'i', type: String },
    { name: 'target', alias: 't', type: String }
];
const args = commandLineArgs(optionDefinitions);
if(!args.index || !args.target) {
    console.error('ERROR: Must specify both index and alias parameters.');
    process.exit(500);
}

// Make a new Elasticsearch client
const elasticNode = args.uri;
let client = getClient(elasticNode);

getAliases(client, args)
.then( (result) => {
    if(result.statusCode == 200) {
        if(result.body.length == 0) { // There is no alias to remove.
            return { statusCode: 200 }
        }
        // If it exists, delete the old alias...
        let current = result.body[0];
        return removeOldAlias(client, current);
    }
    else {
        console.error('GET ERROR: Exit with status code ',result.statusCode);
        process.exit(result.statusCode);
    }
} )
.then( (status) => {
    if(status.statusCode == 200) {
        // Put the new alias...
        let next = {
            index: args.target,
            alias: args.index
        }
        return putNewAlias(client, next);
    }
    else {
        console.error('POST ERROR: Exit with status: ',status);
        process.exit(status.statusCode);
    }
} )
.then( (status) => {
    if(status.statusCode == 200) {
        console.log('SUCCESS!');
        process.exit(0);
    }
    else {
        console.error('POST ERROR: Exit with status code ',status.statusCode);
        process.exit(status.statusCode);
    }
} )
.catch( (err) => console.log( JSON.stringify(err, null, 4) ) );

async function getAliases(client, args) {
    let aliases = await client.cat.aliases({
        name: args.index,
        format: 'json'
    });

    return aliases;
}

async function removeOldAlias(client, old) {
    let result = await client.indices.deleteAlias({
        index: old.index,
        name: old.alias
    });
    return result;
}

async function putNewAlias(client, next) {
    let result = await client.indices.putAlias({
        index: next.index,
        name: next.alias
    });
    return result;
}

function getClient(elasticNode) {
    let client = null;
    try {
        client = new Client({ node: elasticNode, requestTimeout: 60000, maxRetries: 10, sniffOnStart: false, ssl: { rejectUnauthorized: false }, resurrectStrategy: "none", compression: "gzip" })
    }
    catch (e) {
        console.error("getClient",e);
    }
    return client;
}
