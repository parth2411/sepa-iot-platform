import struct, re

def parseDROPLETdata(payload: str):
    # Convert hex string to bytes
    byte_array = bytes.fromhex(payload)

    # > = big endian, h = int16, l = int32
    intTemp, intPress, intHumid, intBatt, intRTCTemp, intRain, intStatus = struct.unpack(">hlhhhhh", byte_array)

    # Scale and convert
    fltTemp = intTemp / 100.0
    fltPress = intPress / 100.0
    fltHumid = intHumid / 100.0
    fltBatt = round(intBatt / 10.0, 2)
    fltRTCTemp = intRTCTemp / 100.0
    fltRain = round(intRain * 0.42, 2)
    fltStatus = intStatus

    return [fltTemp, fltPress, fltHumid, fltBatt, fltRTCTemp, fltRain, fltStatus]

def parseECHOdata(payload: str, emptyDist: int = None):
    # Convert hex string to bytes
    byte_array = bytes.fromhex(payload)

    # > = big endian, h = int16
    intDist, intTemp, intBatt, intWaterTemp, intStatus = struct.unpack(">hhhhh", byte_array)

    #Distance to Level conversion
    if emptyDist is not None:
        fltLevel = float(emptyDist) - intDist
    else:
        fltLevel = float(intDist)

    # Scale and convert
    fltTemp = intTemp / 100.0
    fltBatt = round(intBatt / 1000.0, 2)
    fltWaterTemp = intWaterTemp / 100.0
    fltStatus = intStatus

    return [fltLevel, fltTemp, fltBatt, fltWaterTemp, fltStatus]


def parseHYGROdata(payload: str):
    # Convert hex string to bytes
    byte_array = bytes.fromhex(payload)

    # Unpack payload: > = big endian, H = uint16, h = int16
    intVWC, soilTempRaw, intEC, airTempRaw, humidRaw, battRaw, intStatus = struct.unpack(">HHhHHHH", byte_array)

    # VWC
    rawVWC = intVWC / 10.0
    fltVWC = round((((3.879 / 10000) * rawVWC) - 0.6956) * 100, 2)

    # Soil Temp
    fltSOILTemp = round(soilTempRaw / 100.0, 2)

    # EC
    fltEC = float(intEC)

    # Air Temp
    fltAIRTemp = airTempRaw / 100.0

    # Humidity
    fltHumid = humidRaw / 100.0

    # Battery
    fltBatt = round(battRaw / 1000.0, 2)

    # Status
    fltStatus = intStatus

    return [fltVWC, fltSOILTemp, fltEC, fltAIRTemp, fltHumid, fltBatt, fltStatus]


def parseHydroRangerPayload(strPayload: str = "", emptyDist: int = None):
    # Convert hex string to bytes
    byte_array = bytes.fromhex(strPayload)

    # Unpack data (big-endian: >)
    # b = int8, h = int16
    boolSens, intAvg, intMin, intMax, intTemp, intHumid, intWTemp = struct.unpack(">bhhhhhh", byte_array)
    #Distance to Level conversion
    if emptyDist is not None:
        intLevelAvg = emptyDist - intAvg
        intLevelMax = emptyDist - intMin
        intLevelMin = emptyDist - intMax
    else:
        intLevelAvg = intAvg
        intLevelMin = intMin
        intLevelMax = intMax
        
    # Temperature & humidity scaling
    if intTemp != -777:
        fltTemp = round(intTemp / 100.0, 2)
        fltHumid = round(intHumid / 100.0, 2)
    else:
        fltTemp = None
        fltHumid = None
    #intWTemp for future expansion, not returned here for brevity
    return [boolSens, intLevelAvg, intLevelMin, intLevelMax, fltTemp, fltHumid]

def parseThetaPayload(hex_str):
    ascii_str = bytes.fromhex(hex_str).decode('ascii')
    matches = re.findall(r'[+-][\d.]+', ascii_str)
    rawVWC, TS, ECS = [float(m) for m in matches]
    #return [float(m) for m in matches]
    fltVWC = round((((3.879 / 10000) * rawVWC) - 0.6956) * 100, 2)

    return fltVWC, TS, ECS

#Example DROPLET data parsing
TA, PA, HA, battV, TRTC, TBRTips, Status = parseDROPLETdata('066b000182632710002a000000000000')
print('Droplet example parsed data')
print(f' Air Temperature: {TA} degC    Air Pressure: {PA} mb')
print(f' Air Humidity: {HA} %    Battery Voltage: {battV} vDC')
print(f' RTC chip temperature: {TRTC} degC    Status code: {Status}\n')

#Example ECHO data parsing
level, TRTC, battV, TW, Status = parseECHOdata('05bc00000ed805480000', 1773)
print('Echo example parsed data')
print(f' Water level: {level} mm    RTC chip temperature: {TRTC} degC')
print(f' Battery voltage: {battV} vDC    Water temperature: {TW} degC')
print(f' Status code: {Status}\n')

#Example Hygro data parsing
VWC, TS, ECS, TA, HA, battV, Status = parseHYGROdata('63c70564005009bb206c104900df')
print('Hygro example parsed data')
print(f' Soil Moisture: {VWC} %    Soil Temperature: {TS} degC')
print(f' Soil Electrical Conductivity: {ECS} dS/m    Air Temperature: {TA} degC')
print(f' Air Humidity: {HA} %    Battery voltage: {battV} vDC')
print(f' Status code: {Status}\n')

#Example HydroRanger parsing
sensors, levelAvg, levelMin, levelMax, TA, HA = parseHydroRangerPayload('0508380838083805931e60fc88',2236)
print('HydroRanger example parsed data')
print(f' Sensors: {sensors}    Water level [average]: {levelAvg} mm')
print(f' Water level [minimum]: {levelMin} mm    Water level [maximum]: {levelMax} mm')
print(f' Air Temperature: {TA} degC    Air Humidity: {HA} %\n')

#Example Theta parsing
VWC, TS, ECS = parseThetaPayload('302b323238352e35392b31342e342b32')
print('Theta example parsed data')
print(f' Soil Moisture: {VWC} %    Soil Temperature: {TS} degC')
print(f' Soil Electrical Conductivity: {ECS} dS/m')


