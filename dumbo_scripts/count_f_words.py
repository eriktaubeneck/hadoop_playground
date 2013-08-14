import json

def mapper(key, value):
    msg = json.loads(value)
    user_id = msg['sender_id']
    content = msg['content'].lower()
    yield user_id, content.count('fuck')

def reducer(key, values):
    yield key, sum(values)

if __name__ == '__main__':
    import dumbo
    dumbo.run(mapper, reducer)
