#*******************************************************************************#
# HL7 library file to rapidly parse, receive, and transport HL7 v2.x data       #
# Developed in Python 3.6                                                       #
# July 20, 2018                                                                 #
#*******************************************************************************#
import socket
import datetime
import pickle
from ftplib import FTP
from io import BytesIO, StringIO
from re import compile, match, sub
from os import remove, rename

#-------------------------------------------------------------------------------#
# This function takes the message as a string and creates "msg" variable        #
# in the form of a Python dictionary with HL7 fields as keys and field values   #
# as the values.  Repeating fields and segments are nested python lists         #
#-------------------------------------------------------------------------------#
def parse(raw):
    """Turns message into Python Dictionary"""
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
    ret = "\r"

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
                if com in repetition and currFld != 'MSH.2':
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

    # Adding structure string to dictionary
    msg['build'] = structure

    # Adding a copy of the original message
    msg['raw'] = raw

    # Returning a list of segments
    msg['segments'] = segList
    
    # Useful for scripting to kill or error message
    msg['status'] = ''

    # Line ending, hard coding but can be changed while parsing
    msg['line_ending'] = '\r'

    # Returning short-cuts to useful fields
    msg['msg_date'] = msg['MSH'][0]['MSH.7'][0]['MSH.7.1']
    msg['msg_type'] = msg['MSH'][0]['MSH.9'][0]['MSH.9.1']
    msg['msg_event'] = msg['MSH'][0]['MSH.9'][0]['MSH.9.2']
    msg['msg_id'] = msg['MSH'][0]['MSH.10'][0]['MSH.10.1']
    msg['msg_version'] = msg['MSH'][0]['MSH.12'][0]['MSH.12.1']

    # Returning dictionary
    return msg

#-------------------------------------------------------------------------------#
# Function takes the python dictionary from the "parse" function and turns it   #
# back into a string in the formatted HL7                                       #
#-------------------------------------------------------------------------------#
def toString(msg):
    """Combining Dictionary into HL7 message"""
    if msg == '':
        return False

    # Setting some regex patterns
    fieldRegEx = compile('[A-Z0-9]{3}.([0-9]+)')
    comRegEx = compile('[A-Z0-9]{3}.[0-9]+.([0-9]+)')
    subRegEx = compile('[A-Z0-9]{3}.[0-9]+.[0-9]+.([0-9]+)')

    def order(d,regex):
        """Function takes dictionary of hl7 field names and orders them"""
        ordered = {}
        for k in d:
            n = match(regex,k).group(1)
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
        ret = msg['line_ending']
    else:
        ret = '\r'

    segList = []    # list of repeating segments so we don't go over them twice

    seg_dict = {}   # Keeps count of segments in a dictionary

    segments = msg['build']

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

    # Finished message
    return outMsg

#----------------------------------------------#
# Utilities to use while working with HL7 data #
#----------------------------------------------#
def date(string,format):
    """Takes either date string or generates timestamp and formats"""
    if string.lower() == 'now':
        string = datetime.datetime.now()
        string = datetime.datetime.strftime(string, format)
    else:
        string = datetime.datetime.strptime(string, format)
        string = datetime.datetime.strftime(string, format)
    return string

def get(dic,seg=None,fld=None,com=None,sub=None):
    if sub:
        if isinstance(dic[seg],list):
            if isinstance(dic[seg][0][fld],list):
                return dic[seg][0][fld][0][com][sub]
            else:
                return dic[seg][0][fld][com][sub]
        else:
            if isinstance(dic[seg][fld],list):
                return dic[seg][fld][0][com][sub]
            else:
                return dic[seg][fld][com][sub]
    elif com:
        if isinstance(dic[seg],list):
            if isinstance(dic[seg][0][fld],list):
                return dic[seg][0][fld][0][com]
            else:
                return dic[seg][0][fld][com]
        else:
            if isinstance(dic[seg][fld],list):
                return dic[seg][fld][0][com]
            else:
                return dic[seg][fld][com]
    elif fld:
        if isinstance(dic[seg],list):
            if isinstance(dic[seg][0][fld],list):
                return dic[seg][0][fld][0]
            else:
                return dic[seg][0][fld]
        else:
            if isinstance(dic[seg][fld],list):
                return dic[seg][fld][0]
            else:
                return dic[seg][fld]
    elif seg:
        if isinstance(dic[seg],list):
            return dic[seg][0]
        else:
            return dic[seg]
    else:
        return ''

class table:
    """Class to create and manage table lookups using Python's pickle function"""
    def __init__(self,name):
        # Initializing
        self.name = name

    def create(self,dictionary = {}):
        # Creating pick file
        pickle.dump(dictionary,open(self.name,'wb'))

    def delete(self):
        # Deleting pickle file
        try:
            remove(self.name)
            return True
        except:
            return False

    def lookup(self,key,default):
        # Doing lookup of dictionary
        dictionary = pickle.load(open(self.name,'rb'))
        value = dictionary.get(key,None)
        if value:
            return value
        else:
            return default

    def reverseLookup(self,value,default):
        # Doing reverse lookup, will only fine first match
        dictionary = pickle.load(open(self.name,'rb'))
        for k,v in dictionary.items():
            if v == value:
                return k
        return default

    def read(self):
        # Returning dictionary to user
        dictionary = pickle.load(open(self.name,'rb'))
        return dictionary

#-------------------------------------------------------------------------------#
# Class for inbound TCP functions                                               #
#-------------------------------------------------------------------------------#
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

        def start(self):
            # Initializes and creates socket
            ib = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            ib.settimeout(.01)

            # Binding to address and port
            host = ''
            ib.bind((host,self.port))

            # Starts listener
            ib.listen(1)
            self.ib = ib

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
            ACK = MSH + ret + "MSA" + fld + status + fld + fields[9] + fld + str(error) + ret
                
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
            
            # Initializes and creates socket
            cnxn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cnxn.settimeout(self.timeout)
            self.cnxn = cnxn

        def evaluate(self,ack):
            """Parsing the ACK and evaluating MSA-1"""
            f = ack[3:4]
            if 'MSA'+f+'AA' not in ack:
                return False
            else:
                return True

        def start(self):
            """Connects to remote host"""
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

            return status

        def send(self,message):
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

    def __init__(self,path,filename=None):
        path = path.replace('\\','/')
        self.path = path
        self.filename = filename
        if self.filename:
            # If they supply a filename we get the full path
            self.fullpath = self.path + '/' + self.filename
        else:
            # We use the filepath, assuming they put it there
            self.fullpath = self.path

    def read(self,splitChar = 'MSH'):
        # Reads file and splits HL7 messages
        f = open(self.fullpath,'r')
        data = f.read()
        f.close()

        file.msgList = []

        messages = data.split(splitChar)
        for msg in messages:
            if msg == '':
                continue
            file.msgList.append(splitChar + msg)

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
        try:
            self.f.write(data)
            if not file.appendFlag:
                self.f.close()
            return self
        except:
            return False

    def close(self):
        """Closing file"""
        self.fullpath.close()

    def delete(self):
        """Deleting file after finished"""
        remove(self.fullpath)

    def rename(self,newname):
        """Deleting file after finished"""
        rename(self.fullpath, self.path + '/' + newname)
        self.fullpath = self.path + '/' + newname

    def batch(self,fn='',comments = ''):
        """HL7 batching file"""
        # Reading file
        if not self.filename:
            self.filename = self.fullpath
        temp = open(self.fullpath,'r')
        # If FHS or BHS segments already exist, remove them
        data = temp.read()
        temp.close()
        temp = open(self.fullpath,'w')
        data = data.replace('\n','\r')
        data = sub(r'FHS(.*?)\r|BHS(.*?)\r','',data)
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
        FHSList[8] = self.filename
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
            batch = open(fn,'w')    # They want to write to a new filename
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
        data = sub(r'FHS(.*?)\r|BHS(.*?)\r','',data)
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
            else:
                f = BytesIO(data)
                self.ftp.storbinary("STOR " + destname, f)
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
