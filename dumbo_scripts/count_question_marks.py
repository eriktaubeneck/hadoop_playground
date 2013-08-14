import json

EXCLUDE_COMMITS = True
EXCLUDE_TESTS = True

def mapper(key, value):
    msg = json.loads(value)
    user_id = msg['sender_id']
    content = msg['content']
    stream = msg['display_recipient']
    to_yield = content.count('?')
    if EXCLUDE_COMMITS:
        if stream == 'commits':
            to_yield = 0
    if EXCLUDE_TESTS:
        if stream == 'test-stream':
            to_yield = 0
    yield user_id, to_yield


def reducer(key, values):
    yield key, sum(values)
   
if __name__ == '__main__':
    import dumbo
    dumbo.run(mapper, reducer)
