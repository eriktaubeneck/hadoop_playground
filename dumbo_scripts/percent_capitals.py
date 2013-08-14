import json

EXCLUDE_COMMITS = True
EXCLUDE_TESTS = True

def f(l):
    if l.isupper():
        return 1
    else:
        return 0

def mapper(key, value):
    msg = json.loads(value)
    user_id = msg['sender_id']
    content = msg['content']
    stream = msg['display_recipient']
    chars = list(content)
    
    caps = (sum(map(f, chars)))
    total = len(content)

    if (EXCLUDE_COMMITS and stream == 'commits') or (EXCLUDE_TESTS and stream == 'test-stream'):
        pass
    else:
        yield user_id, (caps, total)

def reducer(key, values):
    caps = total = 0
    for value in values:
        caps += value[0]
        total += value[1]
        
    yield key, float(caps)/total

if __name__ == '__main__':
    import dumbo
    dumbo.run(mapper, reducer)
