import sys

from api04 import run, test

mode = sys.argv[1]

print()
if mode == "run":
    run()
elif mode == "test":
    test()
print()
