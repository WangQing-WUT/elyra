def is_float(text):
    # check for nan/infinity etc.
    if text.isalpha():
        return False
    try:
        float(text)
        return True
    except ValueError:
        return False

def is_bool(text):
    if text.lower() == 'true' or text.lower() == 'false':
        return True
    else:
        return False