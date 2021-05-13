import json
import logging
import pkg_resources
import sys
from math import ceil

import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError


logger = logging.getLogger(sys.argv[0])
INDEX_CREATION_QUERY = '''
    SELECT
        m.mdr_report_key,
        m.event_date as report_date,
        COALESCE(el.name, 'UNKNOWN') AS event_location,
        t.foi_text,
        COALESCE(dop.name, 'UNKNOWN') AS device_operator,
        d.brand_name,
        d.generic_name,
        d.model_number,
        d.catalog_number,
        ma.name AS manufacturer_name,
        COALESCE(ma.name, '') || ' ' || COALESCE(d.brand_name, '') || ' ' || COALESCE(d.generic_name, '')
            || ' ' || COALESCE(d.model_number, '') || ' ' || COALESCE(d.catalog_number, '') AS device_text,
        m.mdr_report_key || '|' || t.mdr_text_key || '|' || t.patient_sequence_number ||
            '|' || de.device_sequence_number AS index_id,
        m.added_date
    FROM
        master_record m INNER JOIN
        foi_text t ON m.mdr_report_key = t.mdr_report_key INNER JOIN
        device_event de on de.mdr_report_key = m.mdr_report_key INNER JOIN
        device d ON d.id = de.device_id INNER JOIN
        manufacturer ma ON m.manufacturer_id = ma.id INNER JOIN
        patient p ON p.mdr_report_key = m.mdr_report_key LEFT OUTER JOIN
        event_location el ON el.code = m.event_location_code LEFT OUTER JOIN
        device_operator dop ON dop.code = de.device_operator_code
    WHERE
      {where}
'''


def get_new_records(start_date, end_date, conn):
    where = f"m.added_date >= '{start_date.strftime('%Y-%m-%d')}' AND m.added_date < '{end_date.strftime('%Y-%m-%d')}'"
    index_creation_query = INDEX_CREATION_QUERY.format(where=where)
    logger.info('Querying MAUDE db for records created between {} and {}'.format(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')))

    df = pd.read_sql(index_creation_query, conn)
    logger.info('Found {} new records'.format(len(df)))

    if len(df) > 0:
        logger.info('removing non-printing unicode characters')
        df.replace({r'[^\x00-\x7F]+': ''}, regex=True, inplace=True)
        df.drop(columns='added_date', inplace=True)
    else:
        df = None

    return df


def get_changed_records(start_date, end_date, conn):
    where = f"""m.change_date >= '{start_date.strftime('%Y-%m-%d')}' AND 
    m.change_date < '{end_date.strftime('%Y-%m-%d')}' AND
    m.added_date < '{start_date.strftime('%Y-%m-%d')}'"""
    index_creation_query = INDEX_CREATION_QUERY.format(where=where)

    logger.info('Querying MAUDE db for records changed between {s} and {e}, and created prior to {s}'.format(
        s=start_date.strftime('%Y-%m-%d'),
        e=end_date.strftime('%Y-%m-%d')))
    df = pd.read_sql(index_creation_query, conn)

    logger.info('Found {} changed records'.format(len(df)))

    if len(df) > 0:
        logger.info('removing non-printing unicode characters')
        df.replace({r'[^\x00-\x7F]+': ''}, regex=True, inplace=True)
        df['split_by'] = df.added_date.map(lambda x: x.strftime('%Y-%m'))
        df.drop(columns='added_date', inplace=True)
        df.sort_values(by='split_by', axis=0, inplace=True)
        df.set_index(keys='split_by', drop=False, inplace=True)
        splits = df.split_by.unique().tolist()
        logger.info(f'Indexes with changed records: {splits}')
        return {s: df.loc[df.split_by == s] for s in splits}

    return None


def create_index(_es, _index_name, replace=False, elastic_version=6):
    index_exists = _es.indices.exists(_index_name)
    logger.info('Index: {} exists: {}'.format(_index_name, index_exists))

    if not index_exists or replace:
        if index_exists and replace:
            logger.info('Deleting existing index: {}'.format(_index_name))
            _es.indices.delete(_index_name)

        schema_file = 'elastic_schema_6-x.json' if elastic_version < 7 else 'elastic_schema_7-x.json'
        with open(pkg_resources.resource_filename('maude_etl', schema_file), 'r') as f:
            schema = json.load(f)
        logger.info('Creating index: {}'.format(_index_name))
        logger.info(schema)
        _es.indices.create(index=_index_name, body=schema, )


def generate_data(_records, _index_name, elastic_version):
    for r in _records:
        _doc = {
            "_index": _index_name,
            "_id": r.get("index_id"),
            "doc": r
        }
        if elastic_version < 7:
            _doc['_type'] = "maude-text"

        print(_doc)

        yield _doc


def create_documents(_es, _records, _index_name, elastic_version):
    logger.info('Creating {} documents'.format(len(_records)))
    try:
        response = helpers.bulk(_es,
                                generate_data(_records, _index_name=_index_name, elastic_version=elastic_version),
                                chunk_size=200,
                                request_timeout=60)
        logger.info('{} documents inserted successfully'.format(response[0]))
    except BulkIndexError as e:
        logger.error(e)


def batch_document_updates(_es, _index_name, documents, batch_size=50000, elastic_version=6):
    if len(documents) > 0:
        number_of_splits = ceil(len(documents) / batch_size)
        logger.info('Splitting new documents into {} batches of {}'.format(
            number_of_splits, batch_size))
        doc_list = np.array_split(documents, number_of_splits)
        for i, docs in enumerate(doc_list):
            logger.info('inserting batch: {} of {}'.format(i, number_of_splits))
            create_documents(_es,
                             _records=docs.drop_duplicates(subset='index_id').to_dict(orient='records'),
                             _index_name=_index_name,
                             elastic_version=elastic_version)
        logger.info('refreshing index: {}'.format(_index_name))
        _es.indices.refresh(index=_index_name)


def update_elasticsearch_indexes(start_date,
                                 end_date,
                                 sql_conn,
                                 es_conn,
                                 elastic_version=6):
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    sqlalchemy_conn = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**sql_conn)
    _records = get_new_records(start_date,
                               end_date,
                               sqlalchemy_conn)
    index_name = 'maude-text-{}'.format(start_date.strftime('%Y-%m'))
    es = Elasticsearch(es_conn)
    create_index(es, index_name, replace=True)
    batch_document_updates(es,
                           index_name,
                           _records,
                           batch_size=20000,
                           elastic_version=elastic_version)

    changed_records = get_changed_records(start_date=start_date,
                                          end_date=end_date,
                                          conn=sqlalchemy_conn)
    if changed_records is not None:
        for i, df in changed_records.items():
            df.drop(columns='split_by', inplace=True)
            logger.info(f'Updating changed documents. index: maude-text-{i}, # documents: {len(df)}')
            batch_document_updates(es,
                                   _index_name=f'maude-text-{i}',
                                   documents=df,
                                   batch_size=20000,
                                   elastic_version=elastic_version)
    else:
        logger.info('No newly changed records to update')
