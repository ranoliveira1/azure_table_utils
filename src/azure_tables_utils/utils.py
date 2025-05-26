from functools import wraps
from typing import List
import copy

def ensure_attributes(*attribute_names):
    '''
    Decorator that verifies if the required attributes have been initialised (different from None)

    Parameters
    ----------
    attribute_names : str
        Name of the attributes to be verified, separated by comman if more than one
    
    Raises
    ------
    AttributeError
        If any of the attibutes has not been initialised (equals to None)
    '''
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            
            for attribute in attribute_names:         
                if getattr(self, attribute) is None:
                    raise AttributeError(f'Attribute "{attribute}" not initialized.')
            return method(self, *args, **kwargs)
        return wrapper
    return decorator


def ensure_non_empty_string(*parameter_names):
    '''
    Decorator that verifies if the provided parameters are non-empty strings

    Parameters
    ----------
    parameter_names : str
        Name of the parameters to be verified, separated by comman if more than one
    
    Raises
    ------
    ValueError
        If any of the parameters is empty or is not a string
    '''
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            for parameter in parameter_names:
                # This first works when the parameter's been provided in this ways "method(name='Rafael')". In this case, kwargs[parameter] equals to 'Rafael'
                if parameter in kwargs:
                    value = kwargs[parameter]
                
                else:
                    # 'method.__code__.co_varnames' is used to return a tuple with all parameters and local variables of a function/method
                    try:
                        idx = method.__code__.co_varnames[1:].index(parameter)
                        value = args[idx] if idx < len(args) else None
                    except ValueError:
                        raise ValueError(f'Parameter "{parameter}" not found in method arguments.')
                
                if not value or not isinstance(value, str):
                    raise ValueError(f'"{parameter}" must be a non-empty string.')
            return method(self, *args, **kwargs)
        return wrapper
    return decorator


def create_entity_batch(entity:List[dict]) -> List:
    count = 0
    batch = []
    temp_batch = []
    
    total_entities = len(entity)

    for idx, item in enumerate(entity, start=1):
        
        temp_batch.append(('upsert', item, {"mode": "replace"}))
        count += 1
        
        if count >= 100 or total_entities == idx:
            batch.append(copy.deepcopy(temp_batch))
            temp_batch.clear()
            count = 0
        
    return batch
