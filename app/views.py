"""
Definition of views.
"""

import json
from django.shortcuts import render
from django.http import HttpRequest
from django.template import RequestContext
from datetime import datetime
from elasticsearch import Elasticsearch

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


def search(index, terms_list):

    terms = []
    for k,v in terms_list.items():
        terms.append(k)
    field_dict = {"field": terms[3] + ".keyword"}
    product_dict = {"terms": field_dict}
    sources_list = [{"product": product_dict}]
    composite_dict = {"sources": sources_list}
    my_buckets_dict = {"composite": composite_dict}
    aggs_dict = {"my_buckets": my_buckets_dict}
    query_dict = {'size': 0, 'aggs': aggs_dict}
    json_string = json.dumps(query_dict)


    #query = r'{ size: 0, "aggs":{ "my_buckets": { "composite" : { "sources" : [ { "product": { "terms" : { "field": "brands_selected.keyword" } } } ] } } } }'
    results = es.search(index=index, body=json_string)
    return results



def home(request):
    """Renders the home page."""
    assert isinstance(request, HttpRequest)

    index_names, index_fields = get_indices()
    selected_index = index_names[0]

    if request.method == 'POST':
        selected_index = request.POST['index_choice']

    selected_index_fields = index_fields[selected_index]

    results = search(selected_index, selected_index_fields)

    return render(
        request,
        'app/index.html',
        {
            'title': 'Home Page',
            'year': datetime.now().year,
            'index_names': index_names,
            'index_fields': selected_index_fields,
            'selected_index': selected_index
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
