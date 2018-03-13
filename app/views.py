"""
Definition of views.
"""

import json
from django.shortcuts import render
from django.http import HttpRequest
from django.template import RequestContext
from datetime import datetime

from app.backends.elasticsearch.elasticsearch_backend import ElasticSearchBackEnd


def home(request):
    """Renders the home page."""
    assert isinstance(request, HttpRequest)

    backend = ElasticSearchBackEnd(request.GET)
    results = backend.query()
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
            # print(bucket)
            dim_dict[dimension].append((bucket['key'][dimension], bucket['doc_count']))
        guided_nav.append(dim_dict)

    return render(
        request,
        'app/index.html',
        {
            'title': 'Home Page',
            'year': datetime.now().year,
            'index_names': backend.index_names,
            'index_fields': backend.selected_index_fields,
            'selected_index': backend.selected_index,
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


def visualize(request):
    """Renders the about page."""
    assert isinstance(request, HttpRequest)

    backend = ElasticSearchBackEnd(request.GET)
    query = backend.visualize_query()

    series: [{
        'name': 'Rating',
        'data': [43934, 52503, 57177, 69658, 97031, 119931, 137133, 154175]
    }]




    return render(
        request,
        'app/visualize.html',
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
