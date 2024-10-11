import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

db = client['Veiculos']

carros_collection = db['Carros']
montadoras_collection = db['Montadoras']


carros = {
    'Modelo': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
    'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']
}

df_carros = pd.DataFrame(carros)

montadoras = {
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford','Honda'],
    'País': ['EUA', 'Alemanha', 'França','EUA','Japão']
}

df_montadoras = pd.DataFrame(montadoras)

def inserir_se_nao_existir(collection, filtro, dados):
    if not collection.find_one(filtro):
        collection.insert_one(dados)
        print(f"Inserido: {dados}")
    else:
        print(f"Dados já existem: {filtro}")

for _, carro in df_carros.iterrows():
    filtro_carro = {'Modelo': carro['Modelo']}
    inserir_se_nao_existir(carros_collection, filtro_carro, carro.to_dict())

for _, montadora in df_montadoras.iterrows():
    filtro_montadora = {'Montadora': montadora['Montadora']}
    inserir_se_nao_existir(montadoras_collection, filtro_montadora, montadora.to_dict())
print("Dados inseridos com sucesso nas collections!")

carros_pipeline = [
    {
        '$lookup': {
            'from': 'Montadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'Montadora_Info'
        }
    },
    {
        '$unwind': '$Montadora_Info'
    },
    {
        '$project': {
            'carro': '$Modelo',
            'cor': '$Cor',
            'montadora': '$Montadora',
            'Montadoras': {'$sum': 1},
            'pais': '$Montadora_Info.País'
        }
    }
]

resultado_carros = list(carros_collection.aggregate(carros_pipeline))

print("\nResultado dos Carros:")
for doc in resultado_carros:
    print(doc)

pais_pipeline = [
    {
        '$lookup': {
            'from': 'Montadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'Montadora_Info'
        }
    },
    {
        '$unwind': '$Montadora_Info'
    },
    {
        '$group': {
            '_id': '$Montadora_Info.País',
            'Carros': {'$sum': 1}
        }
    }
]

resultado_pais = list(carros_collection.aggregate(pais_pipeline))

print("\nResultado da Contagem de Carros por País:")
for doc in resultado_pais:
    print(doc)