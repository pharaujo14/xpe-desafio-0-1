import boto3
import pandas as pd

# Criar um cliente para interagir com o AWS S3
s3_client = boto3.client('s3')

s3_client.upload_file("data/RAIS_VINC_PUB_CENTRO_OESTE.txt","xpe-desafio-mod-01", "raw/RAIS_VINC_PUB_CENTRO_OESTE.txt")
s3_client.upload_file("data/RAIS_VINC_PUB_MG_ES_RJ.txt","xpe-desafio-mod-01", "raw/RAIS_VINC_PUB_MG_ES_RJ.txt")
s3_client.upload_file("data/RAIS_VINC_PUB_NORDESTE.txt","xpe-desafio-mod-01", "raw/RAIS_VINC_PUB_NORDESTE.txt")
s3_client.upload_file("data/RAIS_VINC_PUB_NORTE.txt","xpe-desafio-mod-01", "raw/RAIS_VINC_PUB_NORTE.txt")
s3_client.upload_file("data/RAIS_VINC_PUB_SP.txt","xpe-desafio-mod-01", "raw/RAIS_VINC_PUB_SP.txt")
s3_client.upload_file("data/RAIS_VINC_PUB_SUL.txt","xpe-desafio-mod-01", "raw/RAIS_VINC_PUB_SUL.txt")