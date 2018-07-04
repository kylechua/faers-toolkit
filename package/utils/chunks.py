# Given a list of unspecified size, return it as list of lists of n size
def get(l, n):
    for i in range(0, len(l), n): 
        yield l[i:i + n]