#!/opt/homebrew/bin/python3

S = 0

def test():
    global S
    S += 1

test()
print(S)
