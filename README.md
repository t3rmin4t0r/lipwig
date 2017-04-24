# lipwig
This is a slightly moist clone of [Lipstick](https://github.com/Netflix/Lipstick) built for Hive-on-Tez.

The input format is the output of "explain formatted", which is JSON.
```
lipwig <options> -i explain.json
Options:
	-s|--simple	simple output
	-i|--input	input json file containing apache hive explain plan
	-o|--output	output file
	-t|--type	filetype of output. All types supported by graphviz/dot command
```

or

```
    python lipwig.py [--simple] explain.json > explain.dot
    dot -Tsvg -o explain.svg explain.dot
```
The output ends up looking like this - [query27.svg](http://people.apache.org/~gopalv/q27-plan.svg).
