import json

def mapper(key, value):
    msg = json.loads(value)
    user_id = msg['sender_id']
    name = msg['sender_full_name']
    wordcount = len(msg['content'].split())
    yield (user_id, name), wordcount

def reducer(key, values):
    yield key, sum(values)

if __name__ == '__main__':
    import dumbo
    dumbo.run(mapper, reducer, combiner=reducer)
