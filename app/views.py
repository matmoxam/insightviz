"""
Definition of views.
"""

import json
from django.shortcuts import render
from django.http import HttpRequest
from django.template import RequestContext
from datetime import datetime
from elasticsearch import Elasticsearch


class Filter(object):
    pass

es = Elasticsearch()



def get_indices():
    indices = {}
    index_names = []
    try:

        indices = es.indices.get("*")
        for k, v in indices.items():
            props = v['mappings']['doc']['properties']
            indices[k] = props
            index_names.append(k)
    except Exception as ex:
        print("ERROR: " + str(ex))
    return index_names, indices


def search(index, terms_list, filters_list):

    aggs_dict = {}
    for k, v in terms_list.items():
        field_dict = {"field": k + ".keyword"}
        product_dict = {"terms": field_dict}

        if 'type' in v:
            if v['type'] == 'date':
                field_dict = {"field": k, "interval": "1d"}
                product_dict = {"date_histogram": field_dict}
            elif v['type'] == 'float':
                field_dict = {"field": k}
                product_dict = {"terms": field_dict}
        else:
            print(v)

        sources_list = [{k: product_dict}]
        composite_dict = {"sources": sources_list}
        my_buckets_dict = {"composite": composite_dict}
        aggs_dict[k] = my_buckets_dict

    filter_query_list = []
    bool_dict = { 'bool': {'filter': filter_query_list} }
    for filter in filters_list:
        filter_query_list.append({'term': {filter.key + ".keyword": filter.value}})



    #query_dict = {'size': 0, 'aggs': aggs_dict}
    query_dict = {'aggs': aggs_dict}
    if len(filters_list) > 0:
        query_dict['query'] = bool_dict
    json_string = json.dumps(query_dict)
    # query = r'{ size: 0, "aggs":{ "my_buckets": { "composite" : { "sources" : [ { "product": { "terms" : { "field": "brands_selected.keyword" } } } ] } } } }'
    results = es.search(index=index, body=json_string, timeout="30000ms")
    return results


def home(request):
    """Renders the home page."""
    assert isinstance(request, HttpRequest)

    index_names, index_fields = get_indices()
    selected_index = index_names[0]
    filters_list = []

    if request.method == 'POST':
        selected_index = request.POST['index_choice']
    if 'F' in request.GET:
        vals = request.GET['F'].split(':')
        filt = Filter()
        filt.key = vals[0]
        filt.value = vals[1]
        filters_list.append(filt)


    selected_index_fields = index_fields[selected_index]

    results = search(selected_index, selected_index_fields, filters_list)
    total_records = results['hits']['total']

    guided_nav = []
    results_list = []

    for doc in results['hits']['hits']:
        results_list.append(doc['_source'])

    for item in results['aggregations'].items():
        dimension = item[0]
        dim_dict = {dimension: []}

        buckets = item[1]['buckets']
        for bucket in buckets:
            print(bucket)
            dim_dict[dimension].append((bucket['key'][dimension],bucket['doc_count']))
        guided_nav.append(dim_dict)

    return render(
        request,
        'app/index.html',
        {
            'title': 'Home Page',
            'year': datetime.now().year,
            'index_names': index_names,
            'index_fields': selected_index_fields,
            'selected_index': selected_index,
            'guided_nav': guided_nav,
            'results_list': results_list,
            'total_records': total_records
        }
    )


def contact(request):
    """Renders the contact page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/contact.html',
        {
            'title': 'Contact',
            'message': 'Your contact page.',
            'year': datetime.now().year,
        }
    )


def discover(request):
    """Renders the about page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/discover.html',
        {
            'title': 'Discover',
            'message': 'Search and Find Your Data',
            'year': datetime.now().year,
        }
    )


def exmanager(request):
    """Renders the about page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/exman.html',
        {
            'title': 'Discover',
            'message': 'Search and Find Your Data',
            'year': datetime.now().year,
        }
    )
