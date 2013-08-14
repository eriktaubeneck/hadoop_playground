import json

def mapper(key, value):
    x = json.loads(value)
    yield x['sender_id'], 1

def reducer(key, values):
    yield key, sum(values)

if __name__ == "__main__":
    import dumbo
    dumbo.run(mapper, reducer, combiner=reducer)
