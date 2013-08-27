#!/usr/bin/python
# -*- coding: utf-8 -*-
# author: Toma Kraft
# date: Aug 26th, 2013

import nltk
import sys
import os
import pymongo


# HANDLE DOCUMENT
def process_page(doc, NER_bool):
	if NER_bool:
		entities = named_entities(doc)
		rd = ResolveDict(entities)
		entities = rd.getEntities()
		
		print '[PROCESS_PAGE]\n',entities,'\n',doc
	else:
		print '[PROCESS_PAGE]\n',doc
		
	print doc.keys()
	
	


class MongoBase(object):
	def __init__(self, host=None, port=None, db_name=None, verbose=True):
	

		self.mTag = '[MongoBase]'
		self.verbose = True



		if not host:
			# defualt: 152.15.99.7
			host = 'localhost'
		self.host = host


		if not port:
			# defualt: 27017
			port = '27017'
		self.port = port



		if not db_name:
			# default: twitter
			db_name = 'default'
		self.db_name = db_name

		mongodb_uri = 'mongodb://'+host+':'+port

		try:
			# pymongo objects
			self.conn = pymongo.Connection(mongodb_uri)
			self.db = self.conn[self.db_name]

			if verbose:
				print self.mTag,'successfully connected to:', mongodb_uri, 'using db:', db_name
			
		except:
			print self.mTag,'[CONNECTION ERROR] [__init__]'
			traceback.print_exc(file=sys.stdout)
			self.conn = None


	def close(self):
		if self.conn:
			self.conn.disconnect()

			if self.verbose:
				print self.mTag, 'Closed connection to', self.host,'db:',self.db_name

	def setMtag(self, mTag):
		self.mTag = mTag

	def __str__(self):
		host = self.host
		port = self.port
		db_name = self.db_name
		mTag = self.mTag

		return mTag+' object: '+'mongodb://'+host+':'+port+'  db: '+db_name

	def __repr__(self):
		return self.__str__()

	def __exit__(self):
		self.close()
	
	def __del__(self):
		self.__exit__()
		
	def add(self, database, doc):
		if self.conn:
			self.db[database].insert(doc)
			
		else:
			print self.mTag, '[CONNECTION ERROR] [add]'


# named entity recognition (NER) using nltk
def named_entities(text):
	
	#verbose = True#self.verbose
	
	# GPE = location
	NER_TYPES = ['PERSON','ORGANIZATION','LOCATION','DATE','GPE','FACILITY','PERCENT','MONEY','TIME']
	

	#chunk the new article
	tree = nltk.ne_chunk(
				nltk.pos_tag(
					nltk.word_tokenize(text)))

	# the return list
	# [ (type, Named Entity, Bool ifLocation) ,...]
	entities = []


	#walk through parts of speach hiearchy
	for node in tree:
	
		entity = ''
		
		#if entity then keep track of it
		if isinstance(node, nltk.tree.Tree):
			if node.pos()[-1][-1] in NER_TYPES:#"PERSON" or node.pos()[-1][-1] == "ORGANIZATION":
				name = ""
				for value in node:
					name = name + " " + value[0].strip()
				name = name.strip()
				entity = name

				#####					 #####
				#	DO LOCATION LOOKUP HERE  # 
				#####					 #####

				NERtype = node.pos()[-1][-1]
				if NERtype == 'GPE':
					NERtype = 'LOCATION'

				# look for geocoords
				#if NERtype == 'LOCATION':
					#geo_info = geo.isGeo(entity.lower())
					#entities.append((NERtype, entity, geo_info))
				#else:
				entities.append((NERtype, entity))

				#if verbose:
					#print '[named_entities] extracted entity:', (NERtype, entity)

	#if verbose:
		#print

	return entities


def traverse(folder):	
	#print folder
	for root, subFolders, files in os.walk(folder):
		# files and folders are indexed
		subFolders.sort()
		files.sort()
		
		# traverse any folders
		for s_folder in subFolders:
			#print "%s has subdirectory %s" % (root, s_folder)
			traverse(s_folder)
		
		# print the file 
		for filename in files:
			filePath = os.path.join(root, filename)
			#print "reading: %s" % (filePath)
			with open( filePath, 'rb' ) as f:
				for line in f:
					yield line	
	
def handle_wiki_stream(FOLDER, NER_bool):

	wikipedia = traverse(FOLDER)
	doc = {}
	
	for line in wikipedia:
		line = line.strip()
		
		if line == '</doc>':
			#doc += line + '\n'
			process_page(doc, NER_bool)
				
		elif line[:4] == '<doc' and line[-1] == '>':
			ls_line = line.split()
			
			for item in ls_line:
				ls_item = item.split('=')
				
				if len(ls_item) == 2 :
					key = ls_item[0].replace('"><', '')
					value = ls_item[1].replace('"><', '')
					
					doc[key] = value
			
			doc['text'] = '' 
			doc = ''#''line.strip() + '\n'
		else:
			doc['text'] += line.strip() + '\n'
			
# class to resolve entity names
class ResolveDict(object):
	def __init__(self, entities=None):
		self.__dict__['data'] = {}
		
		if entities:
			self.sortLS(entities)
	
	def sortLS(self, entities):
		ls = []
		for e in entities:
			if len(e) >= 2:
				e_type = e[0]
				name = e[1]
				
				# find how many words are in the name
				name_ls = name.split()
				
				# use count to sort
				count = len(name_ls)
				ls.append((count, name, e_type))
		ls.sort()
		ls.reverse()
		
		for i in ls:
		 	name = i[1]
		 	e_type = i[2]
		 	
		 	self.__setitem__(name, e_type)
	def getEntities(self):
		return self.data
		
	def __iter__(self):
		for key in self.data:
			yield key
			
	def __getitem__(self, key):
		if key in self.data:
			return self.data[key] 
			
	def __setitem__(self, key, value):
		if key in self.data:

			if value in self.data[key]:
				self.data[key][value] += 1
			else:
				self.data[key][value] = 1
		else:
			
			cur = key.split()
			added = False
			
			pvc = []
			pv = self.data.keys()
			for v in pv:
				vls = v.split()
				count = len(vls)
				pvc.append((count,v))
			pvc.sort()
		
			
			for count, possible_val in pvc:
				p_ls = possible_val.split()
				
				if len(cur) > len(p_ls):
					continue
					
				cur_rank = 0
				
				for p in p_ls:
					for c in cur:
						if p == c:

							cur_rank += 1
							
				if cur_rank == len(cur):
					if value in self.data[possible_val]:
						self.data[possible_val][value] += 1
					else:
						self.data[possible_val][value] = 1
					added = True
					break
			
			if not added:
				self.data[key]={}
				self.data[key][value]=1
	
	
def main(args):
	#print '1'
	#m = MongoBase('localhost', '27017', 'wiki_test', True)
	#m.add('wiki_test',{'a':'b'})
	# default
	BASEDIR = os.getcwd()
	FOLDER = os.path.join(BASEDIR, 'clean-text')
	

	
	if '-f' in args:
		FOLDER = os.path.join(BASEDIR, args['-f'])
	
	NER_bool = False
	if '-ner' in args:
		NER_bool = True

	
	
	if FOLDER:
		handle_wiki_stream(FOLDER,NER_bool)
	
if __name__ == '__main__':
	argv = sys.argv[1:]
	args = {}
	if argv:
		args = {}

		# there should be an specifier for each parameter
		# so there should always be an even number of args
		# set the first item to be the specifier of the dict
		# and the second is the value
		if len(argv) % 2 == 0:
			for i in range(len(argv)-1):
				args[argv[i]] = argv[i+1]

	main(args)
