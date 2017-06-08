import sys
import json
import textwrap
from getopt import getopt

from cgi import escape
from itertools import count as counter


SIMPLE=False


def comment(s):
	print "/*\n%s\n*/" % s

def simple():
	global SIMPLE
	return SIMPLE

nextInt = counter().next 

def ifseteq(h, k, v):
	return h.has_key(k) and h[k] == v
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
	def connect(self):
		def drawEdge(a,b):
			print '%s:s -> %s:%s [label="%s", weight=100];' % (a,b,self.port, self.kind)
		if self.srcOp and self.dstOp:
			drawEdge(self.srcOp['OperatorId:'], self.dstOp['OperatorId:'])
		elif self.dstOp:
			drawEdge(self.srcV.bottom, self.dstOp['OperatorId:'])
		elif self.srcOp:
			drawEdge(self.srcOp['OperatorId:'], self.dstV.top)
		else:
			drawEdge(self.srcV.bottom, self.dstV.top)
	def claim(self, vmap, opmap):
		self.srcV = vmap[self.src]
		self.dstV = vmap[self.dst]
		srcops = vmap[self.src].opset
		dstops = vmap[self.dst].opset
		for op in srcops.values():
			if (op.has_key('outputOperator:')):
				# another 1-1 assumption
				outop = op['outputOperator:'][0]
				if dstops.has_key(outop):
					self.srcOp = op
					self.dstOp = dstops[outop]
					if (self.dstOp.has_key('input vertices:')):
						inputs = set(self.dstOp['input vertices:'].values())
						if (self.src in inputs):
							self.port = 'e'
						# do not trust it - no return
		for op in dstops.values():
			if (op.has_key('input vertices:')):
				inputs = set(op['input vertices:'].values())
				if op != self.dstOp and self.dstOp:
					comment("broken explain for " + self.srcOp['OperatorId:'] + " -> " + self.dstOp['OperatorId:']);
				if (self.src in inputs):
					self.dstOp = op
					self.port = 'e'
					return
		if self.kind == "CONTAINS":
			for op in srcops.values():
				if (op.has_key('outputOperator:')):
					# one level deeper
					outop = op['outputOperator:'][0]
					if opmap.has_key(outop):
						finalop = opmap[outop]
						if finalop.has_key("input vertices:"):
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

class TezVertex(object):
	def __init__(self, dag, name, raw):
		self.dag = dag
		self.name = name
		self.raw = raw
		self.vectorized = False
		self.empty = True
		self.parents = []
		self.prefix = name.replace(" ", "_")
		for k in raw:
			if k == "Execution mode:":
				self.vectorized = "vectorized" in raw[k]
			elif k.find("Operator Tree"):
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
		for (k,v) in ops.items():
			if v.items():
				if v.has_key('OperatorId:'):
					yield (v['OperatorId:'],v)
				else:
					v['OperatorId:'] = "FAKE_%d" % (nextInt())
					yield (v['OperatorId:'],v)
			for k1,v1 in v.items():
				if (k1 == "children" and v1): 
					if type(v1) is list:
						for v2 in v1:
							for op in self.getops(v2):
								yield op
					else:
						for op in self.getops(v1):
							yield op

	def draw(self):
		self.nodes = 0
		color = "blue" if self.vectorized else "red"
		print "subgraph cluster_%s {" % self.prefix 
		print 'style=dashed ;' 
		print "compound=true;"
		print "rank=same;"
		print "color=%s;" % color
		print 'label="%s (vectorized=%s)";' % (self.name, str(self.vectorized).lower())
		self.drawOp(self.tree, None)
		print "}"

	def drawOp(self, ops, parent=None):
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
		for (k,v) in ops.items():
			nodeid = self.nodes
			name = "%s" % (v['OperatorId:'])
			self.nodes += 1
			if parent:
				print "%s -> %s [weight=1];" % (parent, name) 
			children = False
			text = ["<tr><td colspan=\"2\"><b>%s</b></td></tr>" % k]
			for k1,v1 in v.items():
				if (k1 == "children" and v1): 
					if type(v1) is list:
						for v2 in v1:
							self.drawOp(v2, name)
					else:
						self.drawOp(v1, name)
				elif k1 == "Statistics:":
					rows = v1[v1.find("Num rows:")+len("Num rows:"):v1.find("Data size:")]
					rawsize = v1[v1.find("Data size:")+len("Data size:") : v1.find("Basic ")]
					text.insert(1,"<tr><td>Rows:</td><td>%s</td></tr>" % rows)
					text.insert(1,"<tr><td>Size:</td><td>%s</td></tr>" % rawsize)
				elif k1 == "alias:" or not simple():
					l = escape(lwrap(json.dumps(v1))).replace("\n", "<br/>")
					comment(l)
					l = l.replace("&lt;s&gt;","<s>").replace("&lt;/s&gt;","</s>");
					if k1 == "predicate:" and l.strip() == '"false (type: boolean)"':
						l='<FONT COLOR="RED" POINT-SIZE="24">&#9888;%s</FONT>' % l
					text.append("<tr><td>%s</td><td>%s</td></tr>" % (lwrap(k1), l))
			#print '%s [label="%s"];' % (name, k)
			if v.items():
				print '%s [shape=plaintext,label=<%s>];' % (name, "<table>%s</table>" % "\n".join(text)) 
			else:
				print '%s [label=<%s>];' % (name, k) 

class HiveTezDag(object):
	def __init__(self, q, raw):
		raw = raw["Tez"]
		self.query = q
		self.name = raw.get("DagName:") or raw.get("DagId:") or "Unknown"
		self.edges = reduce(lambda a,b: a+b, [list(TezEdge.create(k,v)) for (k,v) in ((raw.has_key("Edges:") and raw["Edges:"]) or {}).items()], [])
		self.vertices = [TezVertex(self, k,v) for (k,v) in raw["Vertices:"].items()]
		vmap = dict([(v.name, v) for v in self.vertices])
		opmap = reduce(lambda a,b: a.update(b) or a, [v.opset for v in self.vertices], {})
		comment(opmap.keys())
		# basic assumption 1-1 edge between vertices
		# but connect unions first
		for e in sorted(self.edges, key = lambda e : (e.kind == "CONTAINS" and 0) or 1):
			e.claim(vmap, opmap)
	def draw(self):
		[v.draw() for v in self.vertices]
		[e.connect() for e in self.edges]

class HivePlan(object):
	def __init__(self, q, raw):
		self.raw = raw
		stages = [(k,HiveTezDag(q, v)) for (k,v) in raw["STAGE PLANS"].items() if v.has_key("Tez")]
		assert len(stages) == 1
		self.stages = stages.pop()
	def draw(self):
		print "digraph g {"
		print "node [shape=box];"
		print 'node [id="\N"];'
		print 'compound=true;'
		#print 'splines=ortho;'
		print ""
		self.stages[1].draw()
		print "}"

def main(argv):
	opts, argv = getopt(argv, "0", ['simple'])
	global SIMPLE
	for (k,v) in opts:
		if k == '-0' or k == "--simple":
			SIMPLE=True
	p = [HivePlan(f, json.load(open(f))) for f in argv]
	[x.draw() for x in p]

if __name__ == "__main__":
	main(sys.argv[1:])
