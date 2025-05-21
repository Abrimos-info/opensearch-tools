#!/usr/bin/env node
'use strict';
const { Client } = require("@opensearch-project/opensearch");
const commandLineArgs = require('command-line-args');

const optionDefinitions = [
    { name: 'uri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Opensearch URI
    { name: 'operation', alias: 'o', type: String, defaultValue: 'alias' },
    { name: 'index', alias: 'i', type: String },
    { name: 'target', alias: 't', type: String },
    { name: 'keep', alias: 'k', type: Number, defaultValue: 1 } // How many unaliased versions of index to keep
];
const args = commandLineArgs(optionDefinitions);
if(!args.index || !args.target) {
    console.error('ERROR: Must specify both index and alias parameters.');
    process.exit(500);
}

// Make a new Elasticsearch client
const elasticNode = args.uri;
let client = getClient(elasticNode);

switch(args.operation) {
    case 'reindex':
        executeReindexTools(client, args);
        break;
    case 'alias':
    default:
        executeAliasTools(client, args);
}

function executeReindexTools(client, args) {
    reindexData(client, args)
    .then( (result) => {
        console.log(JSON.stringify(result, null, 4));
    } )
}

function executeAliasTools(client, args) {
    getAliases(client, args)
    .then( (result) => { // REMOVE OLD ALIAS
        if(result.statusCode == 200) {
            if(result.body.length == 0) { // There is no alias to remove.
                return { statusCode: 200 }
            }
            let current = result.body[0];
            return removeOldAlias(client, current);
        }
        else outputError(result.statusCode);
    } )
    .then( (result) => { // PUT NEW ALIAS
        if(result.statusCode == 200) {
            let next = {
                index: args.target,
                alias: args.index
            }
            return putNewAlias(client, next);
        }
        else outputError(result.statusCode);
    } )
    .then( (result) => { // GET OLD INDICES
        if(result.statusCode == 200) {
            return getIndices(client, args);
        }
        else outputError(result.statusCode);
    } )
    .then( (result) => { // REMOVE OLD INDICES
        if(result.statusCode == 200) {
            if(result.body.length <= args.keep) { // No need to delete any indexes
                return { statusCode: 200 }
            }
            else {
                return removeOldIndices(client, args, result.body);
            }
        }
        else outputError(result.statusCode);
    } )
    .then ( (result) => {
        console.log(result);
        process.exit(0);
    } )
    .catch( (err) => console.log( JSON.stringify(err, null, 4) ) );

}


/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */


function outputError(code) {
    console.error('GET ERROR: Exit with status code ',code);
    process.exit(code);
}

async function reindexData(client, args) {
    const response = await client.reindex({
        body: {
            source: {
                index: args.index,
            },
            dest: {
                index: args.target,
            },
        },
        wait_for_completion: false
    });

    return response;
}

async function getAliases(client, args) {
    let aliases = await client.cat.aliases({
        name: args.index,
        format: 'json'
    });

    return aliases;
}

async function getIndices(client, args) {
    let indices = await client.cat.indices({
        index: args.index + '*',
        format: 'json'
    });

    return indices;
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

async function removeOldIndices(client, args, list) {
    let names = [];

    // Extract index names into list, except the index we just aliased
    for(let i=0; i<list.length; i++) {
        let index = list[i];
        if(index.index != args.target) names.push(index.index);
    }
    names.sort(); // Sort alphabetically, which means by date

    for(let j=0; j<names.length - args.keep; j++) {
        await removeIndex(client, names[j]);
    }
    return 'SUCCESS';
}

async function removeIndex(client, index) {
    let result = await client.indices.delete({
        index: index
    })
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
