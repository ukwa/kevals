# kevals
Key-values data aggregator client tools

## Introduction

This library implements a simple data aggregation pattern to help ensure data from multiple sources can be brought together for analysis, and provides an implementation based on Apache Solr [updates](https://lucene.apache.org/solr/guide/6_6/updating-parts-of-documents.html) and [SolrCloud SQL](https://lucene.apache.org/solr/guide/6_6/parallel-sql-interface.html).

The core of the idea is to clearly identify a key (`ID`) that all data sources can know or construct. Then, as events proceed, each source can send updates to the `kevals` database, where the key is shared but the other values are particular to that source.  i.e. it works a little like a [star schema](https://en.wikipedia.org/wiki/Star_schema) with a single 'fact' `ID` and single table with multiple column families.

This simple convention means we can update the data fields from multiple sources, or the same source multiple times, and the results will be stable and correct (i.e. consistent and idempotent). 

For example, we can defined web crawl launch requests based on the seed URL and launch timestamp `ID:<TIMESTAMP>:<SEED>`.  Because our crawl system allows metadata to be added to crawl events, we can use that to attach the launch timestamp, allowing each component to compute the key. Using this shared key:

- The curation system can record when the launch should happen.
- The launcher can record when the launch was initiated.
- The crawl system can record when the launch actually happened.
- The crawl log system can record what happened when the seed was crawled.

The aggregation pattern means we can gather this data even if some transient error means one of the reporting processes is delayed or repeated, and the database can be used to detect any unusual delays or other changes via SQL queries.

## Usage

First, set up a SolrCloud collection. It is recommended that each kind of item is given a different collection, and that an explicit schema is defined for each field for each type.

For development, the supplied scripts can do this, like so:
 
...
