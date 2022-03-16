import sys
import json
import textwrap
from getopt import getopt
from zipfile import ZipFile

from cgi import escape
from itertools import count as counter
from itertools import chain
from collections import defaultdict
from math import log10
from functools import reduce

NX = True
try:
	import networkx as nx
except:
	sys.stderr.write("Could not import nx\npip install networkx, please\n")
	NX = False 

SIMPLE=False

def size_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def comment(s):
	#print "/*\n%s\n*/" % s
	pass

def simple():
	global SIMPLE
	return SIMPLE

nextInt = counter().next

def ifseteq(h, k, v):
	return k in h and h[k] == v
def lwrap(t, n=32):
	return "\n".join(textwrap.wrap(t, n))

class TezEdge(object):
	def __init__(self, src, dst, kind):
		self.src = src
		self.dst = dst
		self.kind = kind
		self.srcV = None
		self.dstV = None
		self.srcOp = None
		self.dstOp = None
		self.port = "n"
	def __repr__(self):
		return "%s -> %s (%s)" % (self.src, self.dst, self.kind) 
	def connect(self):
		label = self.kind
		style = "penwidth=1" 
		if self.srcV.dag.plan.counters:
			ctrname="TaskCounter_%s_OUTPUT_%s" % (self.srcV.name.replace(" ","_"), self.dstV.name.replace(" ", "_"))
			label = "%s" % (self.kind)
			if (ctrname in self.srcV.dag.plan.counters):
				edgectr = self.srcV.dag.plan.counters[ctrname]
				if ('OUTPUT_BYTES_PHYSICAL' in edgectr):
					bytesout = edgectr['OUTPUT_BYTES_PHYSICAL']['counterValue']
					label = "%s (%s)" % (self.kind, size_fmt(int(bytesout))) 
		(s,t,k) = self.srcV.dag.weights.edge2ops(self)
		if self.srcV.dag.weights.iscriticalpath(s,t):
			style = "color=red";
		print('%s:s -> %s:%s [label="%s", weight=100, %s];' % (s,t,self.port, label, style))
	def claim(self, vmap, opmap):
		self.srcV = vmap[self.src]
		self.dstV = vmap[self.dst]
		srcops = vmap[self.src].opset
		dstops = vmap[self.dst].opset
		for op in list(srcops.values()):
			if "Target Vertex:" in op and op["Target Vertex:"] == self.dst:
				if self.kind == 'DPP' and op["OperatorId:"].startswith("EVENT_"):
					self.srcOp = op
					return
			if "outputname:" in op and op["outputname:"] == self.dst:
				self.srcOp = op
			if ('outputOperator:' in op):
				# another 1-1 assumption
				outop = op['outputOperator:'][0]
				if outop in dstops:
					self.srcOp = op
					self.dstOp = dstops[outop]
					if ('input vertices:' in self.dstOp):
						inputs = set(self.dstOp['input vertices:'].values())
						if (self.src in inputs):
							self.port = 'e'
						# do not trust it - no return
		for op in list(dstops.values()):
			if ('input vertices:' in op):
				inputs = set(op['input vertices:'].values())
				if op != self.dstOp and self.dstOp:
					comment("broken explain for " + self.srcOp['OperatorId:'] + " -> " + self.dstOp['OperatorId:']);
				if (self.src in inputs):
					self.dstOp = op
					self.port = 'e'
					return
		if self.kind == "CONTAINS":
			for op in list(srcops.values()):
				if ('outputOperator:' in op):
					# one level deeper
					outop = op['outputOperator:'][0]
					if outop in opmap:
						finalop = opmap[outop]
						if "input vertices:" in finalop:
							inputs = set(finalop['input vertices:'].values())
							if self.dst in inputs:
								self.srcOp = op
								return
		comment("WARNING: No connection for %s->%s" % (self.src, self.dst))
	@staticmethod
	def create(dst, srcs):
		if type(srcs) is dict: srcs = [srcs]
		# tez plan as A <- B, C, D	
		# invert for actual use
		# invert CONTAINS edges
		for s in srcs:
			if s['type'] != "CONTAINS":
				yield TezEdge(s['parent'], dst, s['type'])
			else:
				yield TezEdge(dst, s['parent'], s['type'])
	@staticmethod
	def dpp(src, dst, table):
		return TezEdge(src, dst, 'DPP')	

class TezVertex(object):
	def __init__(self, dag, name, raw):
		self.dag = dag
		self.name = name
		self.raw = raw
		self.vectorized = False
		self.empty = True
		self.prefix = name.replace(" ", "_")
		self.events = []
		self.tree = {}
		self.critical = False
		for k in raw:
			if k == "Execution mode:":
				self.vectorized = "vectorized" in raw[k]
			elif "Operator Tree" in k:
				self.empty = False
				self.tree = raw[k]
				# annoying details Map operator uses a list
				# (tez won't do tagged joins)
				if type(self.tree) is list:
					assert len(self.tree) == 1
					self.tree = self.tree.pop()
		ops = list(self.getops(self.tree))
		self.opset = dict(ops)
		if not self.opset:
			self.opset = {}
			self.top = self.prefix
			self.bottom = self.prefix
			assert "Union" in self.name 
		else:
			self.top = ops[0][0]
			self.bottom = ops[-1][0]
	
	def getops(self, ops):
		if type(ops) is not dict:
			return
		for (k,v) in list(ops.items()):
			if list(v.items()):
				if 'OperatorId:' in v:
					yield (v['OperatorId:'],v)
				else:
					v['OperatorId:'] = "FAKE_%d" % (nextInt())
					yield (v['OperatorId:'],v)
			for k1,v1 in list(v.items()):
				if (k1 == "children" and v1): 
					if type(v1) is list:
						for v2 in v1:
							for op in self.getops(v2):
								yield op
					else:
						for op in self.getops(v1):
							yield op
	def timing(self):
		if (self.events):
			ev = self.events
			start = ev["startTime"]
			end = ev["endTime"]
			prev = start 
			for p in self.dag.parents(self.name):
				x = p.timing()
				if x:
					(s1, s2, e1) = x
					if (prev < e1):
						prev = e1 
			return (start, prev, end) 
		return None
	def draw(self):
		self.nodes = 0
		color = "blue" if self.vectorized else "red"
		print("subgraph cluster_%s {" % self.prefix) 
		print('style=dashed ;') 
		print("compound=true;")
		print("rank=same;")
		print("color=%s;" % color)
		opts = ["vectorized=%s" % str(self.vectorized).lower()]
		t = self.timing()
		if t:
			(s1, s2, e1) = t
			opts.append("own time=%d ms" % (e1-s2))
			if (s1 != s2):
				opts.append("waiting+= %d ms" % (s2-s1)) 
		print('label="%s\\n (%s)";' % (self.name, ", ".join(opts)))
		self.drawOp(self.tree, None, None)
		print("}")

	def op2id(self, op):
		for (k,v) in list(op.items()):
			if ("OperatorId:" in v):
				return v["OperatorId:"] 
	def op2edges(self):
		for opid in self.opset:
			op = self.opset[opid]
			if ("children" in op):
				c = op["children"]
				if type(c) is list:
					for c1 in c:
						yield (opid, self.op2id(c1), 'memory')
				else:
					yield (opid, self.op2id(c), 'memory')
	def drawOp(self, ops, parent=None, prevstats=None):
		important_keys = set([
#			"outputColumnNames:",
			"expressions:",
			"key expressions:",
			"value expressions:",
			"alias:",
			"aggregations:",
			"keys:",
			"Map-reduce partition columns:"
		])
		if type(ops) is not dict:
			return
		children = None
		for (k,v) in list(ops.items()):
			nodeid = self.nodes
			name = "%s" % (v['OperatorId:'])
			self.nodes += 1
			if parent:
				style = ""
				if self.dag.weights.iscriticalpath(parent, name):
					style="color=red"
				print('%s -> %s [weight=1, label="%s" %s];' % (parent, name, prevstats, style)) 
			text = ["<tr><td colspan=\"2\"><b>%s</b></td></tr>" % k]
			for k1,v1 in list(v.items()):
				if (k1 == "children" and v1): 
				    children = v1
				elif k1 == "Statistics:":
					rows = v1[v1.find("Num rows:")+len("Num rows:"):v1.find("Data size:")]
					rawsize = v1[v1.find("Data size:")+len("Data size:") : v1.find("Basic ")]
					if self.dag.plan.counters:
						hivectrs = self.dag.plan.counters["HIVE"]
						if ("RECORDS_OUT_OPERATOR_%s" % name in hivectrs):
							rctr = hivectrs["RECORDS_OUT_OPERATOR_%s" % name]["counterValue"]
						else:
							rctr = -1
						prevdiff = float(rctr)/int(rows)
						prevrows = int(rctr)
						#text.insert(1,"<tr><td>Actual Rows:</td><td>%s (%0.2f)</td></tr>" % (rctr, diff))
					text.insert(1,"<tr><td>Expected Rows:</td><td>%s</td></tr>" % rows)
					text.insert(1,"<tr><td>Size:</td><td>%s</td></tr>" % rawsize)
				elif k1 == "alias:" or not simple():
					l = escape(lwrap(json.dumps(v1))).replace("\n", "<br/>")
					comment(l)
					l = l.replace("&lt;s&gt;","<s>").replace("&lt;/s&gt;","</s>");
					if k1 == "predicate:" and l.strip() == '"false (type: boolean)"':
						l='<FONT COLOR="RED" POINT-SIZE="24">&#9888;%s</FONT>' % l
					text.append("<tr><td>%s</td><td>%s</td></tr>" % (lwrap(k1), l))
			#print '%s [label="%s"];' % (name, k)
			if (self.dag.plan.counters):
				currstats="%s rows (%0.2fx)" % (prevrows, prevdiff)
			else:
				currstats = ""
			if children:
				if type(children) is list:
					for v2 in children:
						self.drawOp(v2, name, currstats)
				else:
					self.drawOp(children, name, currstats)
			if list(v.items()):
				print('%s [shape=plaintext,label=<%s>];' % (name, "<table>%s</table>" % "\n".join(text))) 
			else:
				print('%s [label=<%s>];' % (name, k)) 

class Op2Graph(object):
	def __init__(self, dag):
		self.dag = dag
		edges = dag.edges
		self.weights = None
		self._parents = defaultdict(list)
		self.edges = [(e.src, e.dst, e.kind) for e in dag.edges] 
		for (src,dst,kind) in self.edges:
			self._parents[dst].append(src)
		self.criticals = set()
	def edge2ops(self, e):
		if (e.srcOp and e.dstOp):
			return(e.srcOp['OperatorId:'], e.dstOp['OperatorId:'], e.kind)
		elif e.dstOp:
			return(e.srcV.bottom, e.dstOp['OperatorId:'], e.kind)
		elif e.srcOp:
			return(e.srcOp['OperatorId:'], e.dstV.top, e.kind)
		else:
			return(e.srcV.bottom, e.dstV.top, e.kind)
	def parents(self, vname):
		return self._parents[vname]
	def iscriticalpath(self, s, t):
		if (s in self.criticals) and (t in self.criticals):
			return True
		return False
	def compute(self):
		global NX
		slowest = None
		if NX:
			import networkx as nx
			g = nx.DiGraph()
			op2edges = [self.edge2ops(e) for e in self.dag.edges] + list(chain(*[v.op2edges() for v in self.dag.vertices])) 
			for (s,t,k) in op2edges:
				comment("%s -%s-> %s" % (s,k,t))
			starts = set([src for (src, dst, kind) in op2edges]) - set([dst for (src, dst, kind) in op2edges])
			ends = set([dst for (src, dst, kind) in op2edges]) - set([src for (src, dst, kind) in op2edges])
			for (src,dst,kind) in op2edges:
				g.add_edge(src, dst)
			slowest = []
			e2etime = 0
			def timing(v):
				(s1, s2, e1) = self.dag.vmap[v.name].timing()
				return (e1-s2)
			timings = dict([(v.name, timing(v)) for v in self.dag.vertices if "Union" not in v.name])
			vecops = lambda v: [(op,v.name) for op in list(v.opset.keys())] 
			op2vx = dict(chain(*[vecops(v) for v in self.dag.vertices]))
			comment(op2vx)
			for (s,e) in [(x,y) for x in starts for y in ends]:
				allpaths = nx.all_simple_paths(g, source=s, target=e)
				for path in allpaths:
					vpath = set([op2vx[p] for p in path if "Union" not in p])
					ts = sum([timings[v] for v in vpath])
					if (ts > e2etime):
						e2etime = ts
						slowest = path
			self.criticals = set(slowest)
			comment(slowest)
			comment(self.criticals)

class HiveTezDag(object):
	def __init__(self, plan, q, raw):
		raw = raw["Tez"]
		self.plan = plan
		self.query = q
		self.name = raw.get("DagName:") or raw.get("DagId:") or "Unknown"
		self.edges = reduce(lambda a,b: a+b, [list(TezEdge.create(k,v)) for (k,v) in list((("Edges:" in raw and raw["Edges:"]) or {}).items())], [])
		self.vertices = [TezVertex(self, k,v) for (k,v) in list(raw["Vertices:"].items())]
		self.vmap = dict([(v.name, v) for v in self.vertices])
		opmap = reduce(lambda a,b: a.update(b) or a, [v.opset for v in self.vertices], {})
		comment(list(opmap.keys()))
		for v in self.vertices:
			for k in v.opset:
				if (k.startswith("EVENT_")):
					op = v.opset[k]
					self.edges.append(TezEdge.dpp(v.name,op['Target Vertex:'], op['Target Input:']))
		# basic assumption 1-1 edge between vertices
		# but connect unions first
		for e in sorted(self.edges, key = lambda e : (e.kind == "CONTAINS" and 0) or 1):
			e.claim(self.vmap, opmap)
		self.weights = Op2Graph(self)
	def parents(self, vname):
		return [self.vmap[v] for v in self.weights.parents(vname)]
	def vevents(self, evs):
		for v in self.vertices:
			if (v.name in evs):
				v.events = evs[v.name]
		self.weights.compute()
	def draw(self):
		[v.draw() for v in self.vertices]
		[e.connect() for e in self.edges]

class HivePlan(object):
	def __init__(self, q, raw):
		self.raw = raw
		stages = [(k,HiveTezDag(self, q, v)) for (k,v) in list(raw["STAGE PLANS"].items()) if "Tez" in v]
		stages = [(k,v) for (k,v) in stages if not("File Merge" in v.vmap)]
		assert len(stages) == 1
		self.stages = stages.pop()
		self.counters = {} # none for "explain formatted" 
	def vevents(self, vevents):
		self.stages[1].vevents(vevents)
	def draw(self):
		print("digraph g {")
		print("node [shape=box];")
		print('node [id="\\N"];')
		print('compound=true;')
		#print 'splines=ortho;'
		print("")
		self.stages[1].draw()
		print("}")

def findOneOfThem(nameslist, lookingfor):
	l = [value for value in lookingfor if value in nameslist]
	if l: 
		return l[0]
	return None

def openPackage(f):
	if f.endswith(".zip"):
		with ZipFile(f,'r') as zz:
			jname = findOneOfThem(zz.namelist(), ['DAS/QUERY.json', 'QUERY.json'])
			if not jname:
				# this will throw an error, but we want one
				print('File contains: ', zz.namelist())
				jname = "DAS/QUERY.json"
			qdata = json.loads(zz.read(jname))
			query = qdata['query']
			details = qdata['queryDetails']
			plan = HivePlan(query['queryId'],details['explainPlan'])
			vfile = findOneOfThem(zz.namelist(), ["DAS/VERTICES.json", "DAG0/VERTICES.json", "DAS/VERTEX.json", "DAG0/DAS/VERTEX.json"])
			if vfile:
				vdata = json.loads(zz.read(vfile))
				vevents = dict([(v['name'], v) for v in vdata["vertices"]])
				plan.vevents(vevents)
			# new DAS
			dfile = findOneOfThem(zz.namelist(), ["DAG0/DAS/DAG.json", "DAG0/DAG_INFO.json"])
			if dfile:
				vdata = json.loads(zz.read(dfile))
				dagDetails = vdata["dag"]["dagDetails"]
				details = dagDetails
			# details might come out of DAG details
			if 'counters' in details and details['counters']:
				countergroups = dict([(c['counterGroupName'], dict([(x["counterName"],x) for x in c['counters']])) for c in details['counters']]) 
				plan.counters = countergroups
			return plan
		return None
	return HivePlan(f,json.load(open(f)))

def main(argv):
	opts, argv = getopt(argv, "0", ['simple'])
	global SIMPLE
	for (k,v) in opts:
		if k == '-0' or k == "--simple":
			SIMPLE=True
	p = [openPackage(f) for f in argv]
	[x.draw() for x in p]

if __name__ == "__main__":
	main(sys.argv[1:])
