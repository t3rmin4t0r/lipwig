import sys
import json
import textwrap
import re
from itertools import count as counter
from itertools import chain
from collections import defaultdict
from math import log10
from lipwig import size_fmt, lwrap
from cgi import escape
from getopt import getopt

nextInt = counter().next 

SIMPLE = True 
def simple():
	global SIMPLE
	return SIMPLE

def comment(s):
	#print "/*\n%s\n*/" % s
	pass

def firstchar(a):
	m = re.search('\S', a)
	return m.start()

def indent(l):
	return firstchar(l)/2

class TabNode(object):
	def __init__(self, line, parent=None):
		if line:
			self.indent = indent(line) 
			self.line = line.strip()
		else:
			self.indent = -1
			self.line = None
		self.children = []
		self.parent = parent
	def addchild(self, line):
		cindent = indent(line)
		if (cindent <= self.indent):
			raise "Incorrect indentation"
		child =TabNode(line, self) 
		self.children.append(child)
		return child
	def output(self):
		print "%d:%s%s" % (self.indent, (self.indent)*'--', self.line) 
		for c in self.children:
			c.output()

class CalciteNode(object):
	def __init__(self, kind, options, costs, rows):
		self.id = nextInt()
		self.kind = kind
		self.options = options
		self.costs = costs
		self.rows = rows
		self.tr = lambda l : "<tr><td>%s</td></tr>" % escape(lwrap(l))
	def draw(self):
		node = "node_%d" % (self.id)
		if simple():
			self.drawsimple(node)
		else:
			self.drawfull(node)
		for c in self.children:
			cnode = c.draw()
			estrows = ""
			if (c.rows != -1):
				estrows = '[label="%d rows"]' % (int(c.rows))
			print '%s -> %s %s;' % (cnode, node, estrows)
		return node
	def drawfull(self, node):
		text = ["<tr><td colspan=\"1\"><b>%s</b></td></tr>" % self.kind]
		if (self.options):
			text.append(self.tr(self.options))
		if (self.costs):
			text.append(self.tr(self.costs))
		print '%s [shape=plaintext,label=<%s>];' % (node, "<table>%s</table>" % "\n".join(text))
	def drawsimple(self, node):
		print '%s [shape=record, label="%s"]' % (node, self.kind)

class TableScanNode(CalciteNode):
	ALIAS_PAT=re.compile(r'.*table:alias=\[([^\]]*)\].*')
	#table=[[ge_finance, xx_po_distributions]"
	TABLE_PATH=re.compile(r'table=\[\[([^,]*), ([^,]*)\]\]')
	def __init__(self, kind, options, costs, rows):
		CalciteNode.__init__(self, kind, options, costs, rows)
		m = self.TABLE_PATH.match(self.options)
		self.table="%s.%s" % (m.group(1), m.group(2))
		m = self.ALIAS_PAT.match(self.options)
		self.alias = m.group(1)
	def drawsimple(self, node):
		print '%s [shape=record,label="%s(%s):%s"];' % (node, self.kind, self.alias, self.table)

class JoinNode(CalciteNode):
	JOIN_PAT=re.compile(r'joinType=\[([^\]]*)\]')
	COST_PAT=re.compile(r'cost=.{([^ ]*) rows, ([^ ]*) *cpu, ([^ ]*) *io')
	COND_PAT=re.compile(r'condition=\[(.*)\], joinType') 
	def __init__(self, kind, options, costs, rows):
		CalciteNode.__init__(self, kind, options, costs, rows)
		m = self.JOIN_PAT.search(options)
		self.jointype = m.group(1)
		m = self.COST_PAT.search(options)
		self.joincost = None
		if m:
			self.joincost = int(float(m.group(1))) 
		m = self.COND_PAT.search(options)
		self.typeissue = None
		if m:
			self.typeissue = ('CAST' in m.group(1))
	def drawsimple(self, node):
		costs = ""
		style = ""
		if self.joincost:
			costs = "\\ncost: %d rows (%.2f%%)" % (self.joincost, (100.0*self.rows)/self.joincost)
		if self.typeissue:
			style = "fillcolor=red,style=filled,"
		print '%s [shape=record,%slabel="%s(%s)%s"];' % (node, style, self.kind, self.jointype, costs)

class FilterNode(CalciteNode):
	def __init__(self, kind, options, costs, rows):
		CalciteNode.__init__(self, kind, options, costs, rows)
	def drawsimple(self, node):
		ratio = ""
		before = self.children[0].rows
		if before != -1:
			after = self.rows
			ratio = " (%.2f%%)" % ((100.0*after)/before)  
		print '%s [shape=record,label="%s%s"];' % (node, self.kind, ratio)

class AggregateNode(CalciteNode):
	GROUP_PAT=re.compile(r'group=\[{([^}]*)}\]')
	def __init__(self, kind, options, costs, rows):
		CalciteNode.__init__(self, kind, options, costs, rows)
		m = self.GROUP_PAT.search(options)
		self.groups = len(m.group(1).split(","))-1
	def drawsimple(self, node):
		print '%s [shape=record,label="%s(%d keys)"];' % (node, self.kind, self.groups)

class ProjectNode(CalciteNode):
	def __init__(self, kind, options, costs, rows):
		CalciteNode.__init__(self, kind, options, costs, rows)
		self.cols = len(options.split("=["))-1
	def drawsimple(self, node):
		print '%s [shape=record,label="%s(%d cols)"];' % (node, self.kind, self.cols)
	
class CalciteNodeFactory(object):
	PAT=re.compile(r'([A-Za-z]*)\((.*)\):?(.*)')
	ROW_PAT = re.compile(r'rowcount = ([^,]*),')
	SpecialTypes = {'TableScan' : TableScanNode, 'Join' : JoinNode, 'Filter' : FilterNode, 'Aggregate' : AggregateNode, 'Project' : ProjectNode}
	def create(self, tnode):
		(kind, options, costs, rows) = self.parse(tnode.line)
		node = None
		if (self.SpecialTypes.has_key(kind)):
			node = self.SpecialTypes[kind](kind, options, costs, rows)
		else:
			node = CalciteNode(kind, options, costs, rows)
		node.children = [self.create(c) for c in tnode.children]
		return node
	def parse(self, l):
		if l:
			m=self.PAT.match(l)
			kind = m.group(1).replace("Hive","")
			options = m.group(2)
			costs = m.group(3)
			rows = -1
			if (costs):
				m2 = self.ROW_PAT.search(costs)
				rows = float(m2.group(1))
			return (kind, options, costs, rows)
		return ("RESULT", "", "", -1)

def skipheaders(lines):
	plan = False
	for l in lines:
		if (l.startswith("CBO PLAN:")):
			plan = True
		elif plan and l.strip():
			# skip empty lines
			yield l

def main(args):
	plan = False
	opts, args = getopt(args, "0", ['simple', 'full'])
	global SIMPLE
	for (k,v) in opts:
		if k == '-0' or k == "--simple":
			SIMPLE=True
		if k == "--full":
			SIMPLE=False
	lines = list(skipheaders(open(args[0])))
	root = TabNode(None)
	node = root
	for l in lines:
		cindent = indent(l) 
		if cindent > node.indent:
			node = node.addchild(l)
		elif cindent <= node.indent:
			while node.indent >= cindent:
				# find the right node by popping the stack
				node = node.parent
			node = node.addchild(l)
	factory = CalciteNodeFactory()
	cnode = factory.create(root)
	print """
	digraph g {
	rankdir=BT;
	"""

	cnode.draw()
	print "}"
	


if __name__ == "__main__":
	main(sys.argv[1:])
