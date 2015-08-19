# lipwig
This is a slightly moist clone of [Lipstick](https://github.com/Netflix/Lipstick) built for Hive-on-Tez.

The input format is the output of "explain formatted", which is JSON.

    python lipwig.py [--simple] explain.json > explain.dot
    dot -Tsvg -o explain.svg explain.dot
