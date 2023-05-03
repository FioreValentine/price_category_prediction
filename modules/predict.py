import dill
import os
import logging
import json
import glob

from datetime import datetime
import pandas as pd


def predict():
    os.environ['PROJECT_PATH'] = os.path.expanduser('~/airflow_hw')
    path = os.environ.get('PROJECT_PATH', '.')
    model = ''

    for filenames in os.listdir(f'{path}/data/models/'):
        with open(os.path.join(f'{path}/data/models/', filenames), 'rb') as file:
            model = dill.load(file)
            logging.info('Модель загружена из ' + file.name)

    df_predicted = pd.DataFrame(columns=['id', 'predicted_price_category'])

    for filename in glob.glob(f'{path}/data/test/*.json'):
        with open(filename) as file_json:
            form = json.load(file_json)

            df = pd.DataFrame.from_dict([form])
            y = model.predict(df)
            logging.info('Предсказание сделано для ' + str(form['id']))

            df_tmp = pd.DataFrame([[form['id'], y[0]]], columns=['id', 'predicted_price_category'])

            df_predicted = pd.concat([df_predicted, df_tmp])

    df_predicted.to_csv(f'{path}/data/predictions/preds_{datetime.now().strftime("%Y%m%d%H%M")}.csv')
    logging.info(f'Предсказание записано {path}/data/predictions/')


if __name__ == '__main__':
    predict()
