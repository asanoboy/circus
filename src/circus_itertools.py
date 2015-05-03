
def lazy_chunked(iterator, num):
    stack = []
    for item in iterator:
        stack.append(item)
        if len(stack) == num:
            yield stack
            stack = []
    
    if len(stack):
        yield stack
