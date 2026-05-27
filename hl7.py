#*******************************************************************************#
# HL7 library file to rapidly parse, receive, and transport HL7 v2.x data       #
# Developed in Python 3.6                                                       #
# July 20, 2018                                                                 #
#*******************************************************************************#
import socket
import re
import copy
import sqlite3
import base64
import requests
import json
import time
from ftplib import FTP
from io import BytesIO, StringIO
from os import remove, rename, path, getcwd
from glob import glob
from uuid import uuid4
from datetime import datetime

#---------------------------------------#
#       Class for HL7 manipulation      #
#---------------------------------------#
class parse:
	"""Creating a message object to manipulate"""
	def __init__(self, msg):
		# Initializing the message unparsed
		self.msg = msg
		
		# Parsing message as well
		self.parsedMsg = self.parser()

	def parser(self):
		"""Turns message into Python Dictionary"""
		raw = self.msg
		self.parsedMsg = ''
		
		if raw == '':
			return False

		# This will be the returned parsed message dictionary
		msg = {}

		# Metadata
		structure = []      # Since dictionary loses our order, we maintain in structure string
		segList = []        # List of message segments
		
		# Getting encoding characters from MSH-1 & MSH-2
		fld = raw[3:4]
		com = raw[4:5]
		rep = raw[5:6]
		esc = raw[6:7]
		sub = raw[7:8]
		
		# Finding the newline or return character
		raw = raw.replace('\r\n','\r')
		raw = raw.replace('\n','\r')
		ret = '\r'

		# Splitting Segments at the return character
		segments = raw.split(ret)
		
		# Looping over the segments
		for segment in segments:
			# Getting segment name
			seg = segment[0:3]

			if seg == '':
				continue

			# Adding segment name to segment list
			repSegList = []
			if seg in segList:
				# Checking for repeating segments
				index = segList.index(seg)
				if isinstance(msg[segList[index]],list):
					repSegList = msg[segList[index]]
					msg[seg] = {}
				else:
					repSegList.append(msg[segList[index]])
					msg[seg] = {}
			else:
				# Creating segment dictionary
				msg[seg] = {}
				# Adding unique segment to list
				segList.append(seg)
				# Setting this back to false
				repSegList = []
			
			# Trimming segment name off
			segment = segment[4:]

			# Splitting into fields and assigning to msg dictionary
			fields = segment.split(fld)

			# Looping over fields
			if seg == 'MSH':
				msg[seg]['MSH.1'] = {}
				msg[seg]['MSH.1'] = [{'MSH.1.1':fld}]
				fldCount = 2            # We've already set MSH.1 so we start at 2
			else:
				fldCount = 1

			# Process segments
			for field in fields:
				# This is the current key name field
				currFld = seg+"."+str(fldCount)
				
				# Special handling MSH-2
				if currFld == 'MSH.2':
					msg[seg]['MSH.2'] = [{'MSH.2.1':field}]
					fldCount += 1
					continue
				
				# If field is repeating we loop over repetitions
				field_list = []     # Starting a field list for the repetitions

				repetitions = field.split(rep)

				for repetition in repetitions:
					msg[seg][currFld] = {}

					# Looping over components
					if com in repetition and currFld != 'MSH.2' or sub in repetition and currFld != 'MSH.2':
						comCount = 1
						components = repetition.split(com)

						for component in components:
							currCom = seg+"."+str(fldCount)+"."+str(comCount)

							msg[seg][currFld][currCom] = {}

							# Looping over sub-components
							if sub in component:
								subCount = 1
								subcomponents = component.split(sub)
								for subcomponent in subcomponents:
									currSub = seg+"."+str(fldCount)+"."+str(comCount)+"."+str(subCount)
									msg[seg][currFld][currCom][currSub] = {}
									msg[seg][currFld][currCom][currSub] = subcomponent

									subCount += 1   # Incrementing Sub-Componenet Count
							else:
								msg[seg][currFld][currCom] = component

							comCount += 1   # Incrementing Component Count
					else:
						msg[seg][currFld][currFld + '.1'] = repetition
						
					# Appending field to list
					field_list.append(msg[seg][currFld])

				# Setting the field to a list format    
				msg[seg][currFld] = field_list  

				index = fields.index(field)
				fields[index] = currFld

				fldCount += 1   # Incrementing Field Count Variable 

			# Adding segment name with number of fields to the structure string, used to rebuild the msg later
			structure.append(seg + (fld * len(fields)))

			#if repSegList:
			repSegList.append(msg[seg])
			msg[seg] = repSegList

		# Adding metadata entry for storing useful reference data
		msg['metadata'] = {}

		# Adding structure string to dictionary
		msg['metadata']['build'] = structure

		# Adding a copy of the original message
		msg['metadata']['raw'] = raw

		# Returning a list of segments
		msg['metadata']['segments'] = segList
		
		# Useful for scripting to kill or error message
		msg['metadata']['status'] = ''

		# Line ending, hard coding but can be changed while parsing
		msg['metadata']['line_ending'] = '\r'

		# Returning short-cuts to useful fields
		msg['metadata']['msg_date'] = ''
		msg['metadata']['msg_type'] = ''
		msg['metadata']['msg_event'] = ''
		msg['metadata']['msg_id'] = ''
		msg['metadata']['msg_version'] = ''
		if len(msg['MSH'][0]) >= 7:
			msg['metadata']['msg_date'] = msg['MSH'][0]['MSH.7'][0]['MSH.7.1']
		if len(msg['MSH'][0]) >= 9:
			msg['metadata']['msg_type'] = msg['MSH'][0]['MSH.9'][0]['MSH.9.1']
		if len(msg['MSH'][0]['MSH.9'][0]) >= 2:
			msg['metadata']['msg_event'] = msg['MSH'][0]['MSH.9'][0]['MSH.9.2']
		if len(msg['MSH'][0]) >= 10:
			msg['metadata']['msg_id'] = msg['MSH'][0]['MSH.10'][0]['MSH.10.1']
		if len(msg['MSH'][0]) >= 12:
			msg['metadata']['msg_version'] = msg['MSH'][0]['MSH.12'][0]['MSH.12.1']

		# Returning dictionary
		self.parsedMsg = msg
		return msg

	#-------------------------------------------------------------------------------#
	# Function takes the python dictionary from the "parse" function and turns it   #
	# back into a string in the formatted HL7                                       #
	#-------------------------------------------------------------------------------#
	def toString(self,trim = True):
		"""Combining Dictionary into HL7 message"""
		msg = self.parsedMsg
		
		if msg == '':
			return False

		# Setting some regex patterns
		fieldRegEx = re.compile('[A-Z0-9]{3}.([0-9]+)')
		comRegEx = re.compile('[A-Z0-9]{3}.[0-9]+.([0-9]+)')
		subRegEx = re.compile('[A-Z0-9]{3}.[0-9]+.[0-9]+.([0-9]+)')

		def order(d,regex):
			"""Function takes dictionary of hl7 field names and orders them"""
			ordered = {}
			for k in d:
				n = re.match(regex,k).group(1)
				ordered[int(n)] = k
			l = []
			for k in sorted(ordered):
				l.append(ordered[k])
			return l
		
		# This is the message we will build
		outMsg = ''
		
		# Getting encoding characters
		fld = msg['MSH'][0]['MSH.1'][0]['MSH.1.1']
		com = msg['MSH'][0]['MSH.2'][0]['MSH.2.1'][0:1]
		rep = msg['MSH'][0]['MSH.2'][0]['MSH.2.1'][1:2]
		esc = msg['MSH'][0]['MSH.2'][0]['MSH.2.1'][2:3]
		sub = msg['MSH'][0]['MSH.2'][0]['MSH.2.1'][3:4]
		if 'line_ending' in msg:
			ret = msg['metadata']['line_ending']
		else:
			ret = '\r'

		segList = []    # list of repeating segments so we don't go over them twice

		seg_dict = {}   # Keeps count of segments in a dictionary

		segments = msg['metadata']['build']

		for seg in segments:
			segName = seg[0:3]
			fldSep = seg[3:4]
			
			# Skipping blanks
			if segName == '' or segName not in msg:
				continue

			# Adding field to MSH segment to accomodate MSH.1 being the field separator
			if segName == 'MSH':
				seg += fldSep
				
			# Splitting segment into fields
			fields = seg.split(fldSep)


			if isinstance(msg[fields[0]],list):
				# This is a repeating segment

				# Repeating segment iteration
				t = 0

				if segName in segList:
					t = int(seg_dict[segName])
					t += 1
					seg_dict[segName] = t
				else:
					seg_dict[segName] = t
					segList.append(segName)

				# Adding segment name to beginning of string
				outMsg += segName
				
				# Field iterator
				if segName == 'MSH':
					i = 2   # Need to Start at a higher number for MSH segment
				else:
					i = 1
				while i < len(fields):
					fields[i] = segName+'.'+str(i)
					try:
						if isinstance(msg[fields[0]][seg_dict[segName]][fields[i]],list):
							# If field is a list/repeating field, we keep parsing
							repetitions = []
							x = 0
							for repetition in msg[fields[0]][seg_dict[segName]][fields[i]]:
								repList = []
								if isinstance(repetition,dict):
									# If it is a dictionary then we keep parsing the sub-components
									for c in order(repetition,comRegEx):
										if isinstance(msg[fields[0]][seg_dict[segName]][fields[i]][x][c],dict):
											# Component contains sub-component
											subList = []
											for s in order(msg[fields[0]][seg_dict[segName]][fields[i]][x][c],subRegEx):
												subList.append(msg[fields[0]][seg_dict[segName]][fields[i]][x][c][s])
											repList.append(sub.join(subList))
										else:
											# Appending to field repetition list
											repList.append(msg[fields[0]][seg_dict[segName]][fields[i]][x][c])
								else:
									# No sub-fields in repetition
									repList.append(msg[fields[0]][seg_dict[segName]][fields[i]][x])
								x += 1
								repList = com.join(repList)
								repetitions.append(repList)

							# Adding the repeating field string to the out message with the repetition character
							outMsg += fld + rep.join(repetitions)

						else:
							# Non repeating field
							if isinstance(msg[fields[0]][seg_dict[segName]][fields[i]],dict):
								# Contains components
								comList = []
								for c in order(msg[fields[0]][seg_dict[segName]][fields[i]],comRegEx):
									if isinstance(msg[fields[0]][seg_dict[segName]][fields[i]][c],dict):
										# Contains sub-components
										subList = []
										for s in order(msg[fields[0]][seg_dict[segName]][fields[i]][c],subRegEx):
											subList.append(msg[fields[0]][seg_dict[segName]][fields[i]][c][s])
										comList.append(sub.join(subList))
									else:
										comList.append(msg[fields[0]][seg_dict[segName]][fields[i]][c])
								outMsg += fld + com.join(comList)
							else:
								# Field without components or sub-components
								outMsg += fld + str(msg[fields[0]][seg_dict[segName]][fields[i]])

						# Incrementing count
						i += 1
					except Exception as e:
						i += 1

				# Adding return character back on
				outMsg += ret

			else:
				# Non repeating segment
				
				# Adding segment name to beginning of string
				outMsg += segName

				# field iterator
				if segName == 'MSH':
					i = 2   # Need to Start at a higher number for MSH segment
				else:
					i = 1
				while i < len(fields):
					fields[i] = segName+'.'+str(i)
					try:
						if isinstance(msg[fields[0]][fields[i]],list):
							# If field is a list/repeating field, we keep parsing
							repetitions = []
							x = 0
							for repetition in msg[fields[0]][fields[i]]:
								repList = []
								if isinstance(repetition,dict):
									# If it is a dictionary then we keep parsing the sub-components
									for c in order(repetition,comRegEx):
										if isinstance(msg[fields[0]][fields[i]][x][c],dict):
											# Component contains sub-component
											subList = []
											for s in order(msg[fields[0]][fields[i]][x][c],subRegEx):
												subList.append(msg[fields[0]][fields[i]][x][c][s])
											repList.append(sub.join(subList))
										else:
											# Appending to field repetition list
											repList.append(msg[fields[0]][fields[i]][x][c])
								else:
									# No sub-fields in repetition
									repList.append(msg[fields[0]][fields[i]][x])
								x += 1
								repList = com.join(repList)
								repetitions.append(repList)

							# Adding the repeating field string to the out message with the repetition character
							outMsg += fld + rep.join(repetitions)

						else:
							# Non repeating field
							if isinstance(msg[fields[0]][fields[i]],dict):
								# Contains components
								comList = []
								for c in order(msg[fields[0]][fields[i]],comRegEx):
									if isinstance(msg[fields[0]][fields[i]][c],dict):
										# Contains sub-components
										subList = []
										for s in order(msg[fields[0]][fields[i]][c],subRegEx):
											subList.append(msg[fields[0]][fields[i]][c][s])
										comList.append(sub.join(subList))
									else:
										comList.append(msg[fields[0]][fields[i]][c])
								outMsg += fld + com.join(comList)
							else:
								# Field without components or sub-components
								outMsg += fld + str(msg[fields[0]][fields[i]])

						# Incrementing count
						i += 1
					except Exception as e:
						i += 1

				# Adding return character back on
				outMsg += ret
		
		# If trim is set we remove trailing delimiters and empty segments
		delims = outMsg[0:9]
		if trim:
			fldRx = f'\{fld}+{ret}'
			comRx = f'\{com}+\{fld}'
			comEolRx = f'\{com}+{ret}'
			comRx2 = f'\{com}+\{rep}'
			segRx = '^[A-Z0-9]{3}'+ret+'$'
			outMsg = re.sub(comRx,fld,outMsg)
			outMsg = re.sub(comEolRx,ret,outMsg)
			outMsg = re.sub(fldRx,ret,outMsg)
			# skipping first 10 characters to avoid MSH.1 and MSH.2
			outMsg = delims + re.sub(comRx2,rep,outMsg[9:])
			outMsg = re.sub(segRx,'',outMsg)
		
		# Finished message
		return outMsg
		
	#------------------------------------------#
	# Function to get a value for an HL7 field #
	#------------------------------------------#
	def get(self,field,i=0,j=0):
		msg = self.parsedMsg # This is set in the "parsed" function
		
		if not isinstance(msg,dict):
			return False

		# Splitting the field into the components
		fields = field.split('.')
		seg = ''
		fld = ''
		com = ''
		sub = ''
		if len(fields) == 1:
			seg = fields[0]
		if len(fields) > 1:
			seg = fields[0]
			fld = fields[1]
		if len(fields) > 2:
			com = fields[2]
		if len(fields) > 3:
			sub = fields[3]

		if seg in msg and msg[seg] != None:
			if sub:
				# Returning sub-component
				if f'{seg}.{fld}' in msg[seg][i] and \
				f'{seg}.{fld}.{com}' in msg[seg][i][f'{seg}.{fld}'][j] and \
				f'{seg}.{fld}.{com}.{sub}' in msg[seg][i][f'{seg}.{fld}'][j][f'{seg}.{fld}.{com}']:
					return msg[seg][i][f'{seg}.{fld}'][j][f'{seg}.{fld}.{com}'][f'{seg}.{fld}.{com}.{sub}']
				else:
					return ''
			elif com:
				# Returning field w/o subcomponent
				if f'{seg}.{fld}' in msg[seg][i] and \
				f'{seg}.{fld}.{com}' in msg[seg][i][f'{seg}.{fld}'][j]:
					value = msg[seg][i][f'{seg}.{fld}'][j][f'{seg}.{fld}.{com}']
					if isinstance(value, dict):
						return msg[seg][i][f'{seg}.{fld}'][j][f'{seg}.{fld}.{com}']#[f'{seg}.{fld}.{com}.1']
					else:
						return value
				else:
					return ''
			elif fld:
				# Just a field returned as a list/dict
				if f'{seg}.{fld}' in msg[seg][i]:
					return msg[seg][i][f'{seg}.{fld}']
				else:
					return ''
			elif seg:
				# Returning list/dict of segments
				segReturn = msg[seg]
				if segReturn == None:
					return ''
				else:
					return msg[seg]
			else:
				return ''
		else:
			return ''
	
	#-------------------------------------------#
	# Function to set a value for an HL7 field	#
	#-------------------------------------------#
	def set(self,field,val,i=0,j=0):
		msg = self.parsedMsg
		
		if not isinstance(msg,dict):
			return False

		# Splitting the field into the components
		fields = field.split('.')
		seg = ''
		fld = ''
		com = ''
		sub = ''
		if len(fields) == 1:
			seg = fields[0]
		if len(fields) > 1:
			seg = fields[0]
			fld = fields[1]
		if len(fields) > 2:
			com = fields[2]
		if len(fields) > 3:
			sub = fields[3]
		
		# Functions for formatting/adding fields
		def updateBuild():
			# Updating metadata
			segments = msg['metadata']['build']
			newBuild = []
			for segment in segments:
				segName = segment[0:3]
				if segName == seg:
					fieldSep = segment[3:4]
					newBuild.append(segName + fieldSep * int(fld))
				else:
					newBuild.append(segment)
			msg['metadata']['build'] = newBuild
			
		def addFields():
			for k in range(len(msg[seg][i])+1, int(fld)+1):
				if com and k == int(fld):
					# Adding subfields as needed
					tmpCom = {}
					for l in range(1, int(com)+1):
						tempKey = f'{seg}.{k}.{l}'
						if tempKey == field:
							v = val
						else:
							v = ''
						tmpCom[tempKey] = v
					msg[seg][i][f'{seg}.{k}'] = [tmpCom]
				else:
					tempKey = f'{seg}.{k}.1'
					if tempKey == field:
						v = val
					else:
						v = ''
					msg[seg][i][f'{seg}.{k}'] = [{tempKey: v}]
			updateBuild()# Updating structure
			
		def addComps():
			for k in range(len(msg[seg][i][f'{seg}.{fld}'][j])+1, int(com)+1):
				# Adding subfields as needed
				tempKey = f'{seg}.{fld}.{k}'
				if k == int(com):
					msg[seg][i][f'{seg}.{fld}'][j][tempKey] = val
				else:
					msg[seg][i][f'{seg}.{fld}'][j][tempKey] = ''
		
		def addSubs():
			pass
		
		try:
			if seg in msg and msg[seg] != None:
				if sub:
					msg[seg][i][f'{seg}.{fld}'][j][f'{seg}.{fld}.{com}'][f'{seg}.{fld}.{com}.{sub}'] = val
				elif com:
					if int(fld) > len(msg[seg][i]):
						addFields() # Adding fields to update
					elif isinstance(msg[seg][i][f'{seg}.{fld}'], list) and f'{seg}.{fld}.{com}' not in msg[seg][i][f'{seg}.{fld}'][j]:
						addComps() # Padding components
					else:
						msg[seg][i][f'{seg}.{fld}'][j][f'{seg}.{fld}.{com}'] = val
				elif fld:
					if int(fld) > len(msg[seg][i]):
						addFields() # Adding fields to update
					else:
						msg[seg][i][f'{seg}.{fld}'] = val
				elif seg:
					msg[seg][i] = val
				else:
					msg[seg][i][f'{seg}.{fld}'][j][f'{seg}.{fld}.{com}'] = val
				self.parsedMsg = msg
				return msg
			else:
				return msg
		except KeyError as e:
			key = e.args[0]		
		
	#-------------------------------------------#
	#       Function to clear an HL7 field	    #
	#-------------------------------------------#
	def clear(self,field,i=0,j=0):
		if self.get(field) == None or self.get(field) == '':
			return False
		if isinstance(self.get(field), list):
			self.set(field, [{}])
		elif isinstance(self.get(field), dict):
			self.set(field, {})
		elif isinstance(self.get(field), str):
			self.set(field, '')
			
		return self.parsedMsg
		
	#----------------------------------------------#
	# Utilities to use while working with HL7 data #
	#----------------------------------------------#
	def parsed(self):
		# Returns parsed dictionary message
		return self.parsedMsg
		
	def updateMsg(self, msg):
		# Updating parsed message with new parsed message
		if not isinstance(msg, dict):
			return False
		self.parsedMsg = msg
		return True
	
	def newMsg(self):
		# Function to copy the msg metadata to create a shell without message data
		msg = self.parsedMsg
		tmp = {}
		tmp['metadata'] = copy.deepcopy(msg['metadata'])
		new = copy.deepcopy(self)
		new.parsedMsg = copy.deepcopy(tmp)
		return new
			
	def copyMsg(self):
		# Function to copy the msg metadata and data, complete deep copy
		msg = self.parsedMsg
		new = copy.deepcopy(self)
		new.parsedMsg = copy.deepcopy(msg)
		return new
		
	def addSegment(self,segName,index=None,length=0):
		msg = self.parsedMsg
		if not isinstance(msg, dict):
			return False
			
		# Function to create a new segment
		if length == 0:
			# If they didn't supply a length we default to the length of the MSH segment
			length = len(msg['MSH'][0])
		
		if not index:
			# If no index we stick it at the end
			index = len(msg['metadata']['build']) + 1
		
		# Adding segment to message
		msg['metadata']['build'].insert(index, segName + '|' * length + msg['metadata']['line_ending'])
		
		msg['metadata']['raw'] = ''
		
		# Adding empty fields
		msg[segName] = {}
		for i in range(length):
			msg[segName][f'{segName}.{i+1}'] = {f'{segName}.{i+1}.1':''}
		
		return msg
		
	def copySegment(self, segName, index=-1):
		msg = self.parsedMsg
		if segName in msg:
			if index >= 0 and isinstance(msg[segName], list):
				return copy.deepcopy(msg[segName][index])
			else:
				return copy.deepcopy(msg[segName])
			
	def clearSegment(self,segName,index=-1):
		msg = self.parsedMsg
		if segName in msg:
			segments = msg['metadata']['build']
			if index >= 0 and isinstance(msg[segName], list):
				# First remove from build structure
				si = 0
				for bi,seg in enumerate(segments):
					if seg[0:3] == segName:
						if si == index:
							del segments[bi]
						si += 1
				# If they supply an index remove that one
				del msg[segName][index]
			else:
				for seg in segments:
					if seg[0:3] == segName:
						segments.remove(seg)
				del msg[segName]
			# Updating build
			msg['metadata']['build'] = segments
		
		self.parsedMsg = msg
		return msg
		
	def setSegment(self, segName, segment, index=-1):
		if index >= 0 and isinstance(self.parsedMsg[segName], list):
			self.parsedMsg[segName][index] = segment
		else:
			self.parsedMsg[segName] = segment
		
		return self.parsedMsg
		
	def getSegmentIndex(self, segName, iteration=None):
		segments = self.parsedMsg['metadata']['build']
		iterFlag = False
		first = 0
		last = 0
		for i,seg in enumerate(segments):
			if not iterFlag and seg[0:3] == segName:
				first = i
				iterFlag = True
			if iterFlag and seg[0:3] != segName:
				last = i-1
				break
				
		if iteration:
			if iteration.upper() == 'FIRST':
				return first
			elif iteration.upper() == 'LAST':
				return last
		else:
			return last # Default

#---------------------------------------#
# Class for inbound/outbound TCP socket #
#---------------------------------------#
class tcp:
	"""TCP sender (client) and listener (server) functions"""

	class server():
		"""Class receives data on a listener port on the local machine"""
		def __init__(self,port):
			# Initializes connection object
			self.largeMsg = []   # A variable to hold large messages
			self.ackFlag = True
			self.port = port
			# Connection variables populated when connection is established
			self.conn = None
			self.addr = None
			self.halt = False
			self.qFlag = False
			#self.dbId = self.queue()
			
		def queue(self, name='', db=''):
			# Creates a database queue for the connection
			self.q = database(name, db)
			self.pId = None
			self.qFlag = True

		def start(self):
			# Initializes and creates socket
			ib = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			ib.settimeout(.01)

			# Binding to address and port
			host = ''
			ib.bind((host,self.port))

			# Starts listener
			ib.listen(0)
			self.ib = ib
			
			# Connecting to database
			#if self.qFlag:
			#	self.q.connect()

			def startListener():
				while True:
					# Connecting to client
					if self.halt:
						# They are stopping the connection
						break
					addr = False
					try:
						conn, addr = ib.accept()
					except socket.timeout:
						pass
					if addr:
						# This is the remote IP and port
						self.address = addr
						self.conn = conn
					try:
						data = conn.recv(65536)
					except:
						continue
					if data:    
						if b'\x1c' not in data:
							# Waiting on the end of the message in case its a long message
							self.largeMsg.append(data)        # Converting from byte to string
							continue                            # Keep listening for rest of message
						if self.largeMsg:
							self.largeMsg.append(data)      # Add the last section
							data = b''.join(self.largeMsg)
							self.largeMsg = []

						# Stripping Vertical Tab and File Separators
						data = data.replace(b'\x0b', b'') # Vertical Tab
						data = data.replace(b'\x1c', b'') # File Separator
						data = data.decode('utf-8','ignore')       # Converting from byte to string, ignoring errors
						
						# If queueing is enabled, add to database
						if self.qFlag:
							self.pId = self.q.insert(data)

						# ACK or NACK back
						if self.ackFlag:
							ACK = self.ack(data,'AA')

						# This should be the received HL7 message
						yield data

			self.generator = startListener()

		def ack(self,raw,status,error=''):
			"""Creates AA,AE or AR ACK message and returns it to sender"""
			# First we parse the message
			ACK = ""
				
			# Get the field separator from MSH-1
			fld = raw[3:4]
			fld = raw[3:4]
			com = raw[4:5]

			# Finding the newline or return character
			if "\n" in raw:
				ret = "\n"
			else:
				ret = "\r"

			# Splitting segments
			segments = raw.split(ret)
			# Splitting MSH fields
			fields = segments[0].split(fld)
			i = 0
			MSH = ""
			if len(fields) < 12:
				l = len(fields)
			else:
				l = 12  # We cap at 12
			while i < l:
				if i == 8:
					# Changing MSH-9-1
					coms = fields[i].split(com)
					coms[0] = 'ACK'
					fields[i] = com.join(coms) 
				MSH += fields[i] + fld
				i += 1
			MSH = MSH[0:len(MSH) - 1]# Trimming last field character
			# Combining MSH segment with MSA segment
			# MSA|AA or AE or AR|MSH-10 value
			if error != '':
				ACK = MSH + ret + "MSA" + fld + status + fld + fields[9] + fld + str(error) + ret
			else:
				ACK = MSH + ret + "MSA" + fld + status + fld + fields[9] + ret
				
			# Wraps message and sends outbound
			SB = '\x0b'  # <SB>, vertical tab
			EB = '\x1c'  # <EB>, file separator
			CR = '\x0d'  # <CR>, \r
			FF = '\x0c'  # <FF>, new page form feed
				
			# wrap in MLLP message container
			data = SB + ACK + EB + CR
			data = bytes(data, "utf-8")

			# Sending ACK back on same connection
			self.conn.send(data)

			# Adding to Queue
			if self.qFlag:
				cId = self.q.insert(ACK, self.pId)

			# Returning ACK to use if they do it directly
			return ACK

		def stop(self):
			# Stops the listener
			self.halt = True
			try:
				self.ib.close()
				status = True
			except:
				status = False
				
			# Cleaning up Queue connections
			#if self.qFlag:
			#	self.q.close()

			return status

		def getMsg(self):
			# Getting message from listener
			return next(self.generator)

		def remoteAddress(self):
			# Prints remote address that is connected
			if self.address:
				return self.address

		def autoAck(self,boolian):
			if not boolian:
				self.ackFlag = False
			else:
				self.ackFlag = True

	class client():
		"""Class connects to remote client and sends data"""
		def __init__(self,host,port):
			# Initializes connection object
			self.ackFlag = True
			self.status = False
			self.evalAckFlag = False
			self.timeout = 5
			self.host = host
			self.port = port
			self.qFlag = False
			#self.dbId = self.queue()
			
			# Initializes and creates socket
			cnxn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			cnxn.settimeout(self.timeout)
			self.cnxn = cnxn
			
		def queue(self, name='', db=''):
			# Creates a database queue for the connection
			self.q = database(name, db)
			self.pId = None
			self.qFlag = True

		def evaluate(self,ack):
			"""Parsing the ACK and evaluating MSA-1"""
			f = ack[3:4]
			if 'MSA'+f+'AA' not in ack:
				return False
			else:
				return True

		def start(self):
			"""Connects to remote host"""
			# Connecting to database
			#if self.qFlag:
			#	self.q.connect()
			
			try:
				self.cnxn.connect((self.host, self.port))
				self.status = True
				return True
			except:
				return False

		def restart(self):
			try:
				self.cnxn.close()
			except:
				pass
			try:
				self.cnxn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				self.cnxn.settimeout(self.timeout)
				self.cnxn.connect((self.host, self.port))
				self.status = True
			except:
				self.status = False
				return False

		def stop(self):
			"""Stops the connection"""
			try:
				self.cnxn.close()
				status = True
			except:
				status = False
				
			# Cleaning up Queue connections
			#if self.qFlag:
			#	self.q.close()

			return status
		
		def send(self,message):
			if self.ackFlag:
				ack = False	
				while not ack:
					if not self.status:
						self.restart()
					ack = self.sender(message)
				return ack
			else:
				ret = self.sender(message)
				return ret

		def sender(self,message):
			"""Sends data to outbound TCP connection"""
			# Wraps message and sends outbound
			SB = '\x0b'  # <SB>, vertical tab
			EB = '\x1c'  # <EB>, file separator
			CR = '\x0d'  # <CR>, \r
			FF = '\x0c'  # <FF>, new page form feed
			
			# Wrap in MLLP message container and converts to bytes
			MLLP = SB + message + EB + CR
			msg = bytes(MLLP, "utf-8")
			
			# Sending message
			try:
				self.cnxn.send(msg)
			except Exception as e:
				self.status = False
				return False
				
			# Adding to Queue
			if self.qFlag:
				self.pId = self.q.insert(message, self.pId)

			if self.ackFlag:
				# Storing the ACK
				RECV_BUFFER = 4096
				try:
					ACK = self.cnxn.recv(RECV_BUFFER)   
				except Exception as e:
					self.status = False
					return False
				ACK = ACK.replace(b"\x0b", b"") # Vertical Tab
				ACK = ACK.replace(b"\x1c", b"") # File Separator
				ACK = ACK.decode()

				# Adding to Queue
				if self.qFlag:
					cId = self.q.insert(ACK, self.pId)
					self.pId = None

				# Returning ACK string
				return ACK

		def status(self):
			"""Checking if oubound connection is still open"""
			try:
				self.cnxn.send(b'')
				self.status = True
				return True
			except:
				self.status = False
				return False

		def expectAck(self,boolian):
			"""Setings true or false on whether to expect ACK's.  Default in True"""
			if not boolian:
				self.ackFlag = False
			else:
				self.ackFlag = True

		# def evalAck(self,boolian):
			# """Flag to to evaluate the ACK and if not "AA" we throw an error"""
			# if not boolian:
				# self.evalAckFlag = False
			# else:
				# self.evalAckFlag = True

		def setTimeout(self,timeout):
			"""Setings time timeout on waiting ACK's.  Default is 5 seconds"""
			self.timeout = timeout
			self.cnxn.settimeout(self.timeout)

#---------------------------------------#
#  Class for file Reading and Writing   #
#---------------------------------------#
class file:
	"""File reader designed for reading HL7 files"""
	msgList = []

	def __init__(self,path=None,fn=None):
		if not path:
			path = getcwd()
		path = path.replace('\\','/')
		self.path = path
		self.qFlag = False
		
		self.fn = fn
		if self.fn:
			# If they supply a fn we get the full path
			self.fullpath = self.path + '/' + self.fn
		else:
			# We use the filepath, assuming they put it there
			self.fullpath = self.path
		
		if '*' in self.fullpath:
			# They used a wildcard so use glob to find filname
			self.fullpath = glob(self.fullpath)[0]
			
	def filename(self, fn=''):
		# Used if they wan to dynamically create a fn
		if not fn:
			return False
		path = self.path.replace('\\','/')
		self.path = path
		self.fn = fn
		if self.fn:
			# If they supply a fn we get the full path
			self.fullpath = self.path + '/' + self.fn
		
			
	def queue(self, name='', db=''):
		# Creates a database queue for the connection
		self.q = database(name, db)
		self.pId = None
		self.qFlag = True

	def read(self,splitChar = False):
		# Reads file and splits HL7 messages
		with open(self.fullpath,encoding='utf8',errors='ignore') as f:
			data = f.read()
		f.close()
		file.msgList = []

		if not splitChar:
			# Use regex pattern to capture split characters from standard HL7 format
			splitChar = re.match('MSH[^A-Za-z0-9]{5}',data).group(0)
		messages = data.split(splitChar)
		for msg in messages:
			if msg == '':
				continue
			file.msgList.append(splitChar + msg)
		
			# If queueing is enabled, add to database
			if self.qFlag:
				self.pId = self.q.insert(splitChar + msg)

		return file.msgList

	def open(self,flag='a'):
		try:
			if flag.lower() == 'w':
				self.f = open(self.fullpath,'w')
			else:
				self.f = open(self.fullpath,'a')
			return self
		except:
			return "Unable to write to file %s" % (self.fullpath)

	def write(self,data):
		"""Writing or appending to file"""
		self.open()
		try:
			self.f.write(data)
			
			# If queueing is enabled, add to database
			if self.qFlag:
				self.pId = self.q.insert(data)
				
			if not file.appendFlag:
				self.f.close()
			
			return self
		except:
			return False
	send = write # In case they want to keep it consistent with TCP class

	def close(self):
		"""Closing file"""
		self.fullpath.close()

	def delete(self):
		"""Deleting file after finished"""
		if path.exists(self.fullpath):
			remove(self.fullpath)
		
	def rename(self,newname):
		"""Renaming file after finished"""
		rename(self.fullpath, self.path + '/' + newname)
		self.fullpath = self.path + '/' + newname

	def batch(self,fn='',comments = ''):
		"""HL7 batching file"""
		# Reading file
		if not self.fn:
			self.fn = self.fullpath
		temp = open(self.fullpath,'r')
		# If FHS or BHS segments already exist, remove them
		data = temp.read()
		temp.close()
		temp = open(self.fullpath,'w')
		data = data.replace('\n','\r')
		data = re.sub(r'FHS(.*?)\r|BHS(.*?)\r','',data)
		temp.write(data)
		temp.close()
		messages = self.read()
		# Getting MSH segment from first message
		MSH = messages[0]
		MSH = MSH.replace('\n','\r')
		MSH = MSH.split('\r')
		MSH = MSH[0]
		fld = MSH[3:4]
		total = self.total()

		# Editing FHS segment
		FHSList = MSH.split(fld)
		FHSList[0] = 'FHS'
		FHSList[6] = date('now','%Y%m%d%H%M%S')
		FHSList[8] = self.fn
		FHSList[9] = comments
		FHSList[10] = date('now','%Y%m%d%H%M%S')
		i = 0
		FHS = ''
		while i < 11:
			# We only want first 11 fields
			if i == 10:
				FHS += FHSList[i]
			else:
				FHS += FHSList[i] + fld
			i += 1

		# Editing BHS segment
		BHSList = MSH.split(fld)
		BHSList[0] = 'BHS'
		BHSList[6] = date('now','%Y%m%d%H%M%S')
		BHSList[8] = ''
		BHSList[9] = comments
		BHSList[10] = date('now','%Y%m%d%H%M%S')
		i = 0
		BHS = ''
		while i < 11:
			# We only want first 11 fields
			if i == 10:
				BHS += BHSList[i]
			else:
				BHS += BHSList[i] + fld
			i += 1
		
		# Writing FHS and BHS segments to file with original data
		batch = open(self.fullpath,'r')
		data = batch.read()
		batch.close()
		if fn != '':
			batch = open(fn,'w')    # They want to write to a new fn
		else:
			batch = open(self.fullpath,'w')
		data = data.replace('\n','\r')
		batch.write(FHS+'\r'+BHS+'\r'+data+'BHS'+fld+str(total)+'\r'+'FHS'+fld+'1'+'\r')
		batch.close()

	def debatch(self):
		"""HL7 batching file"""
		# Reading file
		temp = open(self.fullpath,'r')
		# If FHS or BHS segments already exist, remove them
		data = temp.read()
		temp.close()
		temp = open(self.fullpath,'w')
		data = data.replace('\n','\r')
		data = re.sub(r'FHS(.*?)\r|BHS(.*?)\r','',data)
		temp.write(data)
		temp.close()

	def total(self):
		"""Return total of inbound messages"""
		return len(file.msgList)

#---------------------------------------#
#  Class for ftp Reading and Writing    #
#---------------------------------------#
class ftp:
	mode = 'ASCII'
	"""Reading and Writing FTP"""
	def __init__(self,address,port=21):
		"""Initiates FTP object"""
		self.address = address
		self.port = port
		self.ftp = FTP()
		self.qFlag = False
		
	def queue(self, name='', db=''):
		# Creates a database queue for the connection
		self.q = database(name, db)
		self.pId = None
		self.qFlag = True

	def connect(self,usr,pwd):
		"""Creates connection to ftp site"""
		self.ftp.connect(self.address,self.port)
		self.ftp.login(usr,pwd)
		usr = ''
		port = ''
		return self.ftp.getwelcome()

	def cd(self,directory):
		"""Changse directory"""
		self.ftp.cwd(directory)
		return self.ftp.pwd()

	def dir(self):
		"""Returns list of directory items"""
		return self.ftp.dir()

	def send(self,destname,data):
		"""Sending data in either binary or ascii"""
		try:
			if ftp.mode == 'ASCII':
				f = BytesIO(data.encode())
				self.ftp.storlines("STOR " + destname, f)
				# If queueing is enabled, add to database
				if self.qFlag:
					self.pId = self.q.insert(data)
			else:
				f = BytesIO(data)
				self.ftp.storbinary("STOR " + destname, f)
				# If queueing is enabled, add to database
				if self.qFlag:
					self.pId = self.q.insert(data)
			return True
		except:
			return False

	def get(self,destname,splitChar='MSH'):
		"""Gets data in binary or ascii mode"""
		try:
			f = BytesIO()
			self.ftp.retrbinary("RETR " + destname, f.write)
			data = f.getvalue().decode()
			messages = []
			for msg in data.split(splitChar):
				if msg == '':
					continue
				messages.append(splitChar + msg)
				# If queueing is enabled, add to database
				if self.qFlag:
					self.pId = self.q.insert(splitChar + msg)
			return messages
		except:
			return False

	def delete(self,file):
		"""Deletes from from ftp host"""
		try:
			self.ftp.delete(file)
			return True
		except:
			return False

	def rename(self,old,new):
		"""Renames from on ftp host"""
		try:
			self.ftp.rename(old,new)
			return True
		except:
			return False

	def setMode(self,mode):
		"""Changes mode to either BINARY or ASCII"""
		if mode.upper() == 'BINARY':
			ftp.mode = 'BINARY'
		else:
			ftp.mode = 'ASCII'
		return ftp.mode

	def close(self):
		"""Closes FTP connection"""
		self.ftp.quit()

#---------------------------------------#
# Class for SQLite3 Reading and Writing #
#---------------------------------------#
class queue:
	"""Reading and Writing SQLite3 Database"""
	def __init__(self, name='', db=''):
		# Creates a database queue for the connection
		self.q = database(name, db)
		self.qId = self.q.getId(name)
		self.pId = None
		self.qFlag = True
		
	def getMsg(self):
		# Getting top message with Processed flag = 0
		row = self.q.query()
		#print(row, self.pId)
		while not row:
			row = self.q.query()
			continue
		self.pId = row[0]
		encodedMsg = row[1]
		msg = base64.b64decode(encodedMsg.encode()).decode()
		self.updateMsg(self.pId)
		return msg	
		
	def updateMsg(self, id):
		self.q.update(id)
		return True
		
	def send(self, msg):
		self.q.insert(msg)
		return True

#---------------------------------------#
#       Class for HTTPS Requests        #
#---------------------------------------#
class rest:
	"""HTTPS Requests Class"""
	def __init__(self):
		# Setting queue to negative
		self.qFlag = False
		
		# Initializing variables
		self.url = ''
		self.resource = ''
		self.key = ''
		self.secret = ''
		
		# Defaulting auto-refreshing the JWT token
		self.refresh_token = True
		
		# Initializing json variables
		self.headers = ''		# If it requires specific headers
		self.parameters = '' 	# Query parameters
		self.body = '' 			# Certain calls use JSON body
		self.jwt = ''
		
		# Starting session
		self.session = requests.session()
		
	def queue(self, name='', db=''):
		# Creates a database queue for the connection
		self.q = database(name, db)
		self.pId = None
		self.qFlag = True
		
	def basic(self, key, secret):
		"""Used for basic authentication"""
		# Creating authentication string
		auth_string = key + ":" + secret	# Concatenating key and secret w/ ":"
		self.auth_string = base64.b64encode(auth_string.encode()).decode()	# Base64 encoding
		
		return self.auth_string
		
	def oauth(self, auth_url, key, secret):
		"""OAuth2 authenticating"""
		
		# Creating authorization using basic method
		self.basic(key, secret)
		
		# Setting OAuth parameters
		header_params = {
			"Authorization": "Basic "+self.auth_string,
			"Content-Type": "application/x-www-form-urlencoded"
		}
		
		# Setting body in a different variable
		self.oauth_body = self.body
		self.body = ''
		
		# Requesting Token
		token_resp = self.session.post(url=auth_url,headers=header_params,data=self.oauth_body)

		# Processing token
		if token_resp.status_code == 200:
			# Retreiving Token
			self.jwt = json.loads(token_resp.text)
			
			# Setting token expiration to check against minus 10 seconds as a buffer
			self.expiration = time.time() + int(self.jwt['expires_in']) - 10

			# Building Header for Patient Search
			headerData = 'Bearer '+self.jwt['access_token']
			self.headers = {'Authorization':headerData,'Accept':'application/json'}
			return self.jwt
		else:
			return token_resp.text
	
	def check_token(self):
		"""Checks OAuth token and refreshes"""
		if time.time() >= self.expiration:
			self.oauth()
		return time.time() - self.expiration
			
	def get(self, base_url, resource=None):
		"""Calling API"""
		
		if resource:
			self.resource = resource
			self.url = f'{self.url}/{resource}'
			
		self.url = base_url
		
		# Verifying token and refreshing if needed
		if self.refresh_token:
			x = self.check_token()
		
		# Processing parameters for searches
		if self.parameters:
			if isinstance(self.parameters, list):
				self.parameters = '&'.join(self.parameters) # Combining parameters into URL list
			
			url = f'{self.url}/{self.resource}?{self.parameters}'
		elif self.resource:
			url = f'{self.url}/{self.resource}'
		else:
			url = f'{self.url}'
		
		# Adding to Queue
		if self.qFlag:
			self.pId = self.q.insert(message, self.pId)
			
		resp = self.session.get(url,headers=self.headers)
		print(resp.request)
		print(resp.text)
		
		return resp.text

	def post(self):
		"""Calling API"""
		# Verifying token and refreshing if needed
		if self.refresh_token:
			x = self.check_token()
		
		# Processing parameters for searches
		if self.parameters:
			if isinstance(self.parameters, list):
				self.parameters = '&'.join(self.parameters) # Combining parameters into URL list
			
			url = f'{self.url}?{self.parameters}'
		
		resp = self.session.post(url,headers=self.headers)
		
		return resp

#---------------------------------------#
#  Class SQLite database logging for    #
#  Connections above				    #
#  Not meant to be called directly		#
#---------------------------------------#
class database:
	"""Uses SQLite for logging message"""
	def __init__(self, tblName='', dbName='', days=30):
		self.qId = str(uuid4())[24:36] # Create GUID (last section)
		# Saving database name
		if not dbName:
			dbName = 'queues.db'
		elif not bool(re.match(r'.+\..+$', dbName)) and dbName != ':memory:':
			# Add file extension
			dbName += '.db'
		if not tblName:
			tblName = self.qId # Default to GUID
		tblName = tblName.upper()

		# Connecting to database
		self.conn = sqlite3.connect(dbName)
		self.cursor = self.conn.cursor()
		
		# Setting table name
		txtName = tblName
		tblName = 'Q_' + self.qId
		
		# Create controller table if needed
		queue_schema = f"""
			CREATE TABLE IF NOT EXISTS queues (
			ID INTEGER PRIMARY KEY AUTOINCREMENT
			,strInstanceId TEXT
			,strName TEXT
			,dtAdded DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
			,dtUpdated DATETIME
			,intPurgeDays INTEGER NOT NULL DEFAULT {days}
			,intActive BOOLEAN NOT NULL DEFAULT 1
		)
		"""
		self.cursor.execute(queue_schema)
		self.cursor.execute(f'SELECT strInstanceId FROM queues WHERE strName=\'{txtName}\'')
		row = self.cursor.fetchone()
		if row:
			# If it is an existing entry return the row
			self.qId = row[0]
			tblName = 'Q_' + self.qId
			#return self.qId
		else:
			# Create the new entry and table
			self.cursor.execute(f'INSERT INTO queues (strInstanceId,strName) VALUES (\'{self.qId}\',\'{txtName}\')')
			self.conn.commit()
			
			# Create queue table if it doesn't exist
			msgs_schema = f"""
				CREATE TABLE IF NOT EXISTS \'{tblName}\' (
				ID INTEGER PRIMARY KEY AUTOINCREMENT
				,txtMsg TEXT
				,dtAdded DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
				,dtUpdated DATETIME
				,intProcessed BOOLEAN NOT NULL DEFAULT 0
				,intParentId INTEGER
			)
			"""
			self.cursor.execute(msgs_schema)
			
		# Disabling journal and sync for speed
		self.cursor.execute('PRAGMA journal_mode=MEMORY;')
		self.cursor.execute('PRAGMA synchronous=NORMAL;')
		self.conn.commit()
		
		#return self.qId
	
	def getId(self, name, db=''):
		name = name.upper()
		if not db:
			db = 'queues'
		sql = f'SELECT strInstanceId FROM {db} WHERE strName="{name}" AND intActive=1'
		self.cursor.execute(sql)
		row = self.cursor.fetchone()
		return row[0]
		
	def insert(self, msg, parent=None):
		tblName = 'Q_' + self.qId
		encodedMsg = base64.b64encode(msg.encode()).decode()	# Base64 encoding message
		sql = f'INSERT INTO "{tblName}" (txtMsg, intParentId) VALUES (?, ?)'
		params = (encodedMsg, parent)
		self.cursor.execute(sql, params)
		lastId = self.cursor.lastrowid
		self.conn.commit()
		
		return lastId
		
	def query(self, id=None):
		tblName = 'Q_' + self.qId
		sql = f'SELECT ID, txtMsg FROM {tblName} WHERE intProcessed=0 ORDER BY ID LIMIT 1'
		self.cursor.execute(sql)
		row = self.cursor.fetchone()
		return row
		
	def update(self, id):
		tblName = 'Q_' + self.qId
		sql = f'UPDATE {tblName} SET intProcessed=1,dtUpdated=CURRENT_TIMESTAMP WHERE ID={id}'
		self.cursor.execute(sql)
		self.conn.commit()
		return True
		
	def close(self):
		# Closing connections
		self.cursor.close()
		self.conn.close()
		
	def pruner(self, name=''):
		# Pruning messages
		pass
		
	def export(self, name):
		# Exporting queue
		pass
