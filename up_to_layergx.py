from layers_gx import LayersGX
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
load_dotenv()
s3_access_key = os.getenv('AWS_KEY')
s3_secret_key = os.getenv('AWS_SECRET')
aws_region = 'us-east-1'

# BETA
# url_layers_gx = 'https://api.staging.gaivota.ai/api/layers-gx/v1'

# PROD
url_layers_gx = 'https://api-internal.gaivota.ai/api/layers-gx/v1'


key_name = {

    'agricultural_capability_high': 'alta_br',
    'agricultural_capability_low': 'baixa_br',
    'agricultural_capability_moderate': 'moderada_br'

}

path = 'gaivota-data-science/files_aptidao/br_fix'
for i in key_name:
    print(i)

    s3_path = '{}/{}.geojson'.format(
        path, key_name[i])
    geom_type_str = 'Polygon'
    data_type_str = 'Public'
    include_gaivota_id = 'Y'

    date_file = (datetime.now() + timedelta(4)).strftime('%Y-%m-%d')

    lgx = LayersGX()

    kwargs = {
        'aws_path_s3': s3_path,
        'aws_access_key': s3_access_key,
        'aws_security_key': s3_secret_key,
        'aws_region': aws_region,
        'low_zoom_detail': 12,
        'high_zoom_detail': 14,
    }

    data = lgx.insert_layers_gx(
        url_layers_gx,
        i,
        geom_type_str,
        data_type_str,
        date_file,
        include_gaivota_id,
        **kwargs
    )
    print(data)
