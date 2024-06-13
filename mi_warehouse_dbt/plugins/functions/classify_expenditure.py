from typing import List

import cloudpickle
from duckdb.typing import DuckDBPyType


def load_model_dict():
    with open('../data/ml_models/zero_shot_model.pkl', 'rb') as f:
        model_dict = cloudpickle.load(f)
    return model_dict


def classify_expenditure(text_list: List[str], threshold: float) -> DuckDBPyType(list[str]):
    output = []
    model_dict = load_model_dict()
    for text in text_list:
        prediction = model_dict['run_prediction'](text, model_dict['classifier'], model_dict['labels'])
        if prediction['predicted_prob'] > threshold:
            output.append(prediction['predicted_label'])
        else:
            output.append("unknown")
    return output