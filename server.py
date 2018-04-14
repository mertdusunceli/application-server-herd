import asyncio
import string
import time 
import urllib.request
import json
import aiohttp
import async_timeout
import sys
import logging 

# "Goloman": 18190, "Hands": 18191, "Holiday": 18192, "Wilkes": 18193, "Welsh": 18194

API_KEY = 'AIzaSyDgDkOicr6WtxY1ddWowwgP_pDdVqRPagM'

class Server:
    def __init__(self, ID):
        self.serverID = ID
        self.clientDict = {}
        self.clientTimeDiff = {}
        self.clientServer = {}
        self.clientFlood = {}
        self.currentClientID = ""
        self.serverDict = {"Goloman": ["Hands", "Holiday", "Wilkes"], 
                            "Hands": ["Wilkes", "Goloman"], 
                            "Holiday": ["Welsh", "Wilkes", "Goloman"],
                            "Wilkes": ["Goloman", "Hands", "Holiday"], 
                            "Welsh": ["Holiday"]}
        self.serverPort = {"Goloman": 18190, "Hands": 18191, "Holiday": 18192, "Wilkes": 18193, "Welsh": 18194}
        self.serverList = self.serverDict[self.serverID]

    def createClient(self, clientID, location, time):                   #create a new client       
        self.clientDict[clientID] = (location, time)
        self.clientServer[clientID] = self.serverID
        self.clientFlood[clientID] = True

    def updateClientDict(self, clientID, location, time):
        currentTime = self.clientDict[clientID][1]
        if (time > currentTime):
            logging.info("need to update client")
            self.clientDict.update({clientID: (location, time)})        #update the client if the new info is time-wise newer
            self.clientServer.update({clientID: self.serverID}) 
            self.clientFlood.update({clientID: True})           
        else:
            logging.info("no need to update client")
            self.clientFlood.update({clientID: False})

    async def handle_iamat(self, clientID, location, time):
        if (clientID in self.clientDict):
            self.updateClientDict(clientID, location, time)
        else:
            logging.info("need to create client")
            self.createClient(clientID, location, time)
        currentTime = self.clientDict[clientID][1]
        timeDiff = self.findTimeDiff(currentTime)                   #find the timediff and save it with a client 
        if (clientID in self.clientTimeDiff): 
            self.clientTimeDiff.update({clientID: timeDiff})
        else: 
            self.clientTimeDiff[clientID] = timeDiff
        result = self.buildIamatMsg(clientID)
        return result      

    def buildIamatMsg(self, clientID):
        currentTime = self.clientDict[clientID][1]
        currentLocation = self.clientDict[clientID][0]          #builds iamat message 
        timeDiff = self.clientTimeDiff[clientID]
        message = "AT " + self.clientServer[clientID] + " " + timeDiff + " " + clientID + " " + currentLocation + " " + ("%.9f" % currentTime)
        return message

    def findTimeDiff(self, currentTime):                    #finds time difference once when the client is created or updated
        serverTime = time.time()
        if (serverTime > currentTime):
            timeDiff = serverTime - currentTime
            formatTimeDiff = "+" + ("%.9f" % timeDiff)
        else:
            timeDiff = currentTime - serverTime
            formatTimeDiff = "-" + ("%.9f" % timeDiff)
        return formatTimeDiff


    def createClientForAt(self, clientID, location, time, serverID, timeDiff):                   #create a new client 
        self.clientDict[clientID] = (location, time)
        self.clientServer[clientID] = serverID
        self.clientTimeDiff[clientID] = timeDiff
        self.clientFlood[clientID] = True

    def updateClientDictForAt(self, clientID, location, time, serverID, timeDiff):
        currentTime = self.clientDict[clientID][1]
        if (time > currentTime):
            logging.info("IN FLOOD: need to update client")
            self.clientDict.update({clientID: (location, time)})        #update the client if the new info is time-wise newer
            self.clientServer.update({clientID: serverID})
            self.clientTimeDiff.update({clientID: timeDiff})
            self.clientFlood.update({clientID: True})
        else:
            logging.info("IN FLOOD: no need to update client")
            self.clientFlood.update({clientID: False})

    async def handle_at(self, serverID, timeDiff, clientID, location, time):
        if (clientID in self.clientDict):
            self.updateClientDictForAt(clientID, location, time, serverID, timeDiff)
        else:
            logging.info("IN FLOOD: need to create client")  
            self.createClientForAt(clientID, location, time, serverID, timeDiff)
        return    

    def findLatitude(self, currentLocation):
        s = ""
        s += currentLocation[0]
        i = 1
        while not currentLocation[i] == '+' and not currentLocation[i] == '-':
            s += currentLocation[i]
            i += 1
        if (s[0] == '+'):
            latitude = s[1:]
        else: 
            latitude = s 
        return latitude

    def findLongitude(self, currentLocation):
        i = 1
        while not currentLocation[i] == '+' and not currentLocation[i] == '-':
            i += 1
        s = currentLocation[i:]
        if (s[0] == '+'):
            longitude = s[1:]
        else: 
            longitude = s 
        return longitude 

    async def buildWhatsatMsg(self, latitude, longitude, radius, infoAmount): 
        places = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=" + latitude + "," + longitude + "&radius=" + radius + "&key=" + API_KEY
        async with aiohttp.ClientSession() as session:
            data = await self.NearbySearch(session, places)
        data["results"] = data["results"][:infoAmount]
        data = json.dumps(data, sort_keys = True, indent = 4)
        return data         

    async def NearbySearch(self, session, places):
        async with async_timeout.timeout(10):
            async with session.get(places) as response:
                return await response.json()

    async def handle_whatsat(self, clientID, radius, infoAmount):               #creates whatsat message 
        if (int(radius) > 50 or int(infoAmount) > 20):
            logging.error("ERROR: out of bounds")  
            result = "ERROR: out of bounds"       
        elif (not (clientID in self.clientDict)):
            logging.error("ERROR: no such client")
            result = "ERROR: no such client"
        else: 
            actualr = str(1000*(int(radius)))
            currentLocation = self.clientDict[clientID][0] 
            latitude = self.findLatitude(currentLocation)
            longitude = self.findLongitude(currentLocation)
            newline = '\n'
            result = self.buildIamatMsg(clientID) + newline + str(await self.buildWhatsatMsg(latitude, longitude, actualr, int(infoAmount)))
        return result

    async def flooding(self, message):
        for i in self.serverList:
            port = self.serverPort[i]
            logging.info("started flooding for: %r" % i) 
            await tellThem(message, port)
            logging.info("ended flooding for: %r" % i)

    async def handle_message(self, message):
        pieces = message.split()
        msglength = len(pieces)
        if (msglength >= 4): 
            if (pieces[0] == "IAMAT" and msglength == 4):
                self.currentClientID = pieces[1]
                logging.info("Started IAMAT message")
                answer = await self.handle_iamat(pieces[1], pieces[2], float(pieces[3]))
                if (self.clientFlood[self.currentClientID]):
                    logging.info("flooding IAMAT")
                    await self.flooding(answer)
                else:
                    None
                logging.info("Finished IAMAT message")
            elif (pieces[0] == "WHATSAT" and msglength == 4):
                logging.info("Started WHATSAT message")
                answer = await self.handle_whatsat(pieces[1], pieces[2], pieces[3])
                logging.info("Finished WHATSAT message")
            elif (pieces[0] == "AT" and msglength == 6):
                logging.info("Started AT message")
                self.currentClientID = pieces[3]
                await self.handle_at(pieces[1], pieces[2], pieces[3], pieces[4], float(pieces[5]))    
                answer = message
                if (self.clientFlood[self.currentClientID]):
                    logging.info("flooding AT")
                    await self.flooding(answer)
                else:
                    None
                logging.info("Finished AT message")
            else: 
                answer = "? " + message
                logging.info("answer")
        else:
            answer = "? " + message  
            logging.info("answer")
        return answer

    async def handle_input(self, reader, writer):
        while(not reader.at_eof()):
            try:
                data = await reader.read(1000)                       
                message = data.decode()  
            except:
                logging.error("Error: Could not read from the file")
            finally:
                if (message != ""):
                    logging.info("Received %r" % message)
                    answer = await self.handle_message(message)
                    logging.info("Send: " + answer)
                    encoded = answer.encode()
                    writer.write(encoded)
                    await writer.drain()

        logging.info("Close the client socket")       
        writer.close()

async def tellThem(message, port):
    try:            
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(message.encode())
        logging.info("Closed this inter communication")
        writer.close()
    except ConnectionRefusedError:
        logging.error("ConnectionRefusedError")

def main(): 
    name = sys.argv[1]
    server = Server(name)                                  
    fname = name + ".log" 
    logging.basicConfig(filename=fname, level=logging.DEBUG)
    logging.info(server.serverID)
    logging.info(server.serverPort[server.serverID])
    logging.info(server.serverList)
    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(server.handle_input, '127.0.0.1', server.serverPort[server.serverID], loop=loop)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    main()
