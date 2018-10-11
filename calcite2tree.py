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

SIMPLE = False
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
			text.append(tr(self.options))
		if (self.costs):
			text.append(tr(self.costs))
		print '%s [shape=plaintext,label=<%s>];' % (node, "<table>%s</table>" % "\n".join(text))
	def drawsimple(self, node):
		print '%s [shape=record, label="%s"]' % (node, self.kind)

class TableScanNode(CalciteNode):
	ALIAS_PAT=re.compile(r'.*table:alias=\[([^\]]*)\].*')
	def __init__(self, kind, options, costs, rows):
		CalciteNode.__init__(self, kind, options, costs, rows)
		m = self.ALIAS_PAT.match(self.options)
		self.alias = m.group(1)
	def drawsimple(self, node):
		print '%s [shape=record,label="%s(%s)"];' % (node, self.kind, self.alias)
	
class CalciteNodeFactory(object):
	PAT=re.compile(r'([A-Za-z]*)\((.*)\):?(.*)')
	ROW_PAT = re.compile(r'rowcount = ([^,]*),')
	"""
	HiveJoin(condition=[true], joinType=[inner], algorithm=[none], cost=[not available])
	"""
	SpecialTypes = {'TableScan' : TableScanNode}
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
		return ("ROOT", "", "", -1)

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
	opts, args = getopt(args, "0", ['simple'])
	global SIMPLE
	for (k,v) in opts:
		if k == '-0' or k == "--simple":
			SIMPLE=True
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
