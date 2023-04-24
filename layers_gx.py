import requests


class LayersGX():
    def __init__(self) -> None:
        pass

    @staticmethod
    def create_entity(base_url, entity_name):
        url_create_entity = f'{base_url}/entity'
        req = requests.post(url_create_entity, json={'name': entity_name})
        if req.ok:
            entity_id = req.json()['id']

            return {entity_name: entity_id}

        return {'error': f'Could not create entity. '
                         f'HTTP STATUS {req.status_code} {req.content}'}

    @staticmethod
    def delete_entity(base_url, entity_id):
        url_delete_entity = f'{base_url}/entity/{entity_id}'
        req = requests.delete(url_delete_entity)
        if req.ok:
            print(f'Entity Deleted! {entity_id}')
            return {entity_id: True}

        return {'error': f'Could not delete entity. '
                         f'HTTP STATUS {req.status_code} {req.content}'}

    @staticmethod
    def get_entity_by_name(base_url, entity_name, amount=20, page=0):
        params = {
            'amount': amount,
            'page': page,
            'value': entity_name,
            'search_by': 'name',
        }

        url_search_entity = f'{base_url}/entity'
        req = requests.get(url_search_entity, params=params)
        if req.ok:
            try:
                entity_id = [
                    x for x in req.json() if x['name'] == entity_name
                ][0]['id']
                return {entity_name: entity_id}
            except:
                pass
        return {'error': f'Could not find entity. '
                         f'HTTP STATUS {req.status_code} {req.content}'}

    @staticmethod
    def get_entity_information(base_url, entity_id):
        url_entity = f'{base_url}/entity/{entity_id}'
        req = requests.get(url_entity)
        if req.ok:
            info = req.json()
            return {entity_id: info}
        return {'error': f'Could not get entity information. '
                         f'HTTP STATUS {req.status_code} {req.content}'}

    @staticmethod
    def get_entity_layer_operations(
        base_url, entity_name, amount=20, page=0
    ):
        params = {
            'amount': amount,
            'page': page,
            'value': entity_name,
            'search_by': 'name',
        }
        params = '&'.join(params)
        url_entity = f'{base_url}/layers-operations?{params}'
        req = requests.get(url_entity, params=params)
        if req.ok:
            try:
                info = req.json()
                operations = [
                    x for x in info if x["entity"]["name"] == entity_name
                ]
                return {entity_name: operations}
            except:
                pass
        return {'error': f'Could not get entity layer operations info. '
                         f'HTTP STATUS {req.status_code} {req.content}'}

    @staticmethod
    def search_entity_metadata(
        base_url: str,
        entity_id: str,
        search_by: str,
        value: str,
        operator: str,
        fields: list,
        amount=20,
        page=0
    ):
        if not operator in ['like', 'equal']:
            return {'error': 'Operator must be either like or equal'}

        params = {
            'amount': amount,
            'page': page,
            'fields': ','.join(fields),
            'entity_id': entity_id,
            'value': value,
            'search_by': search_by
        }
        url_entity = f'{base_url}/metadata-geometry/entity/{entity_id}'
        req = requests.get(url_entity, params=params)
        if req.ok:
            try:
                info = req.json()
                return {entity_id: info}
            except:
                pass
        return {'error': f'Could not search entity layer metadata. '
                         f'HTTP STATUS {req.status_code} {req.content}'}

    @staticmethod
    def delete_layer_operation(base_url, layer_operation_id):
        url_delete_operation = f'{base_url}/layers-operations/' \
                               f'{layer_operation_id}'
        req = requests.delete(url_delete_operation)
        if req.ok:
            return {layer_operation_id: True}

        return {'error': f'Could not delete layer operation. '
                         f'HTTP STATUS {req.status_code} {req.content}'}

    @staticmethod
    def get_geom_type_code(geom_type_str):
        if geom_type_str == "Polygon":
            return 1
        elif geom_type_str == "Line":
            return 2
        elif geom_type_str == "Point":
            return 3

    @staticmethod
    def get_data_type_code(data_type_str):
        if data_type_str == "Public":
            return 1
        elif data_type_str == "Private":
            return 2
        elif data_type_str == "Proprietary":
            return 3

    @staticmethod
    def post_layer_operation(
        base_url,
        entity_id,
        type_data,
        geom_type,
        date_file,
        include_gaivota_id,
        **kwargs
    ):

        if (not 'aws_path_s3' in kwargs
                and not 'geojson_file' in kwargs):
            return {'error': 'You must provide either aws_s3_path or '
                             'geojson_file path parameter'}

        url_entity_upload = f'{base_url}/layers-operations'

        data = {
            "entity_id": entity_id,
            "type_data": type_data,
            "date_file": date_file,
            "geom_type": geom_type,
            "include_gaivota_id": include_gaivota_id
        }

        data = {**data, **kwargs}

        if 'aws_path_s3' in data:

            req = requests.post(url_entity_upload, data=data)

            return {entity_id: req.json()}

        files = {
            "geojson_file": open(data['geojson_file'], 'rb')
        }
        data.pop('geojson_file')
        req = requests.post(url_entity_upload, data=data, files=files)
        return {entity_id: req.json()}

    def insert_layers_gx(
        self,
        base_url,
        entity_name,
        geom_type_str,
        data_type_str,
        date_file,
        include_gaivota_id,
        **kwargs
    ):
        entity = self.get_entity_by_name(
            base_url, entity_name)

        if 'error' in entity:
            entity = self.create_entity(base_url, entity_name)

        assert entity_name in entity, f'Entity id is None, cannot proceed. reason:{entity}'
        entity_id = entity[entity_name],

        layer_id = self.post_layer_operation(
            base_url=base_url,
            entity_id=entity_id,
            type_data=self.get_data_type_code(data_type_str),
            geom_type=self.get_geom_type_code(geom_type_str),
            date_file=date_file,
            include_gaivota_id=include_gaivota_id,
            **kwargs
        )

        return {entity_name: {
            'entity_id': entity_id,
            'layer_operation': layer_id[entity_id]}
        }
