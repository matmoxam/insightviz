from elasticsearch import Elasticsearch
import json


class DocField:

    def __init__(self, k, v):
        self.name = k
        self.type = "Dynamic"
        self.analyzed_name = k

        # Use keyword fields for text fields
        if 'type' in v:
            self.type = v['type']
            if v['type'] == 'text':
                self.analyzed_name = self.name + '.keyword'


class ElasticSearchBackEnd:

    PARAM_SEARCH = 'S'
    PARAM_FILTER = 'F'
    PARAM_FILTER_KEY = 'FK'
    PARAM_FILTER_VALUE = 'FV'
    PARAM_AGGREGATE = 'A'
    PARAM_INDEX = 'I'
    PARAM_X = 'X'
    PARAM_Y = 'Y'

    def __init__(self, query_params):
        self.client = Elasticsearch()
        self.index_names, self.indices = self.get_indices()
        self.nav_state = {}
        self.num_of_filters = 1
        self.selected_index = self.index_names[0]
        self.selected_index_fields = []
        self.agg_X_fields = []
        self.agg_Y_fields = []
        self.set_nav_state(query_params)
        self.set_index_fields()

    def query(self):
        query_string = self.get_query_string()
        return self.client.search(index=self.selected_index, body=query_string, timeout="30000ms")

    def get_numeric_fields(self):
        field_list = []
        for field in self.selected_index_fields:
            if field.type == "float" or field.type == "long" or field.type == "int":
                field_list.append(field)
        return field_list

    def set_index_fields(self):
        fields_dict = self.indices[self.selected_index]
        for k, v in fields_dict.items():
            self.selected_index_fields.append(DocField(k, v))

    def get_field_by_name(self, name):
        for field in self.selected_index_fields:
            if field.name == name:
                return field
        return None

    def get_text_fields(self):
        fields = []
        for field in self.selected_index_fields:
            if field.type == "text":
                fields.append(field.name)
        return fields

    def get_analyzed_field_name(self, name):
        for field in self.selected_index_fields:
            if field.name == name:
                return field.analyzed_name
        return name

    def set_nav_state(self, query_params):
        if ElasticSearchBackEnd.PARAM_SEARCH in query_params:
            self.nav_state[ElasticSearchBackEnd.PARAM_SEARCH] = query_params[ElasticSearchBackEnd.PARAM_SEARCH]

        for i in range(1, len(query_params)):
            key = ElasticSearchBackEnd.PARAM_FILTER_KEY + str(i)
            value = ElasticSearchBackEnd.PARAM_FILTER_VALUE + str(i)
            if key in query_params and value in query_params:
                self.nav_state[ElasticSearchBackEnd.PARAM_FILTER + str(i)] = (query_params[key], query_params[value])
                self.num_of_filters += 1

        if ElasticSearchBackEnd.PARAM_AGGREGATE in query_params:
            self.nav_state[ElasticSearchBackEnd.PARAM_AGGREGATE] = query_params[ElasticSearchBackEnd.PARAM_AGGREGATE]

        if ElasticSearchBackEnd.PARAM_INDEX in query_params:
            self.selected_index = query_params[ElasticSearchBackEnd.PARAM_INDEX]

        if ElasticSearchBackEnd.PARAM_X in query_params:
            x_fields = query_params[ElasticSearchBackEnd.PARAM_X].split("|")
            for field in x_fields:
                self.agg_X_fields.append(self.get_field_by_name(field))

        if ElasticSearchBackEnd.PARAM_Y in query_params:
            y_fields = query_params[ElasticSearchBackEnd.PARAM_Y].split("|")
            for field in y_fields:
                self.agg_Y_fields.append(self.get_field_by_name(field))




    def get_search_dict(self):
        """ Gets a dictionary for the search term syntax that will be converted into a query string
        :rtype: Dictionary
        """

        match = {"match_all": {}}
        if ElasticSearchBackEnd.PARAM_SEARCH in self.nav_state:
            match = {'multi_match': {'query': self.nav_state[ElasticSearchBackEnd.PARAM_SEARCH], 'fields': self.get_text_fields()}}
        return match

    def get_filter_list(self):
        filter_list = []
        for i in range(1, self.num_of_filters):
            key = ElasticSearchBackEnd.PARAM_FILTER + str(i)
            dimension = self.get_analyzed_field_name(self.nav_state[key][0])
            dimension_value = self.nav_state[key][1]
            filter_list.append({"term": {dimension: dimension_value}})
        return filter_list

    def get_all_field_agg_buckets(self):
        all_buckets = {}
        for field in self.selected_index_fields:
            all_buckets[field.name] = {'composite': {'size': 10, 'sources': [{field.name: {'terms': {'field': field.analyzed_name}}}]}}
        return all_buckets

    def get_viz_agg_list(self):
        field_list = []
        if ElasticSearchBackEnd.PARAM_AGGREGATE in self.nav_state:
            nav_fields = self.nav_state[ElasticSearchBackEnd.PARAM_AGGREGATE].split("|")
            for field_name in self.agg_axis_fields:
                field = self.get_field_by_name(field_name)
                field_list.append(field)
        return field_list

    def get_agg_bucket(self):
        field_list = self.agg_X_fields + self.agg_Y_fields
        sources_list = []
        bucket = {"viz_bucket": {'composite': {'size': 20000, 'sources': sources_list}}}
        for field in field_list:
            source = {field.name: {'terms': {'field': field.analyzed_name}}}
            sources_list.append(source)
        return bucket

    def get_query_string(self):
        query_dict = {'bool': {"filter": self.get_filter_list(), 'must': self.get_search_dict()}}
        aggs_dict = self.get_all_field_agg_buckets()
        query_dict = {'query': query_dict, 'aggs': aggs_dict}
        json_string = json.dumps(query_dict)
        return json_string

    def get_visualization_query_string(self):
        query_dict = {'bool': {"filter": self.get_filter_list(), 'must': self.get_search_dict()}}
        aggs_dict = self.get_agg_bucket()
        query_dict = {'size': 0, 'query': query_dict, 'aggs': aggs_dict}
        json_string = json.dumps(query_dict)
        return json_string

    def visualize_query(self):
        if len(self.agg_X_fields) > 0 and len(self.agg_Y_fields) > 0:
            query_string = self.get_visualization_query_string()
            return self.client.search(index=self.selected_index, body=query_string, timeout="30000ms")
        else:
            return None

    def get_indices(self):
        indices = {}
        index_names = []
        try:
            indices = self.client.indices.get("*")
            for k, v in indices.items():
                if not k == ".kibana":
                    props = v['mappings']['doc']['properties']
                    indices[k] = props
                    index_names.append(k)
        except Exception as ex:
            print("ERROR: " + str(ex))
        return index_names, indices