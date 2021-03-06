import json

def mapper(key, value):
    msg = json.loads(value)
    user = msg['sender_full_name']
    wordcount = len(msg['content'].split())
    yield user, wordcount

def reducer(key, values):
    yield key, sum(values)

if __name__ == '__main__':
    import dumbo
    dumbo.run(mapper, reducer, combiner=reducer)
