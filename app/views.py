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
    query_results = backend.visualize_query()
    numeric_fields = backend.get_numeric_fields()
    all_fields = backend.selected_index_fields
    x_list = []
    y_list = []
    if query_results:
        query_results = query_results['aggregations']['viz_bucket']['buckets']
        for bucket in query_results:
            print(bucket)
            values_dict = bucket['key']
            x_list.append(values_dict[backend.agg_X_fields[0].name])
            y_list.append(values_dict[backend.agg_Y_fields[0].name])

        x_list.sort()
        y_list.sort()

    xAxis: {
        "categories": x_list
    }


    series = [{
        'name': "Y AXIS",
        'data': y_list
    }]

    return render(
        request,
        'app/visualize.html',
        {
            'series': series,
            'selected_index': backend.selected_index,
            'y_axis': numeric_fields,
            'x_axis': all_fields,
            'x_series': x_list,
            'y_series': y_list,
            'index_names': backend.index_names
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
