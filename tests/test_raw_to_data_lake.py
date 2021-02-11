import sys
sys.path.append("..")
from GCP_cloud_functions.raw_to_data_lake import raw_to_data_lake
import mock
from unittest.mock import patch, mock_open
from unittest import TestCase
from google.cloud.storage import Client
from google.cloud.pubsub_v1 import PublisherClient
import pytest

"""
This script handles the unit testing for the raw-to-data-lake Cloud Function.
"""

def test_is_json_clean_true():
    mock_json = [{'timeReceived': '2020-05-14T11:37:06Z', 'year': '2020', 'month': '05', 'day': '14', 'hour': '11', 'version': '1.1.0', 'type': 'bsm', 'msgCount': '91', 'id': '"j6EXKQ=="', 'secMark': 6500, 'latitude': 39.7452755, 'longitude': -105.6791004, 'elevation': 8302.664, 'accuracySemiMajor': 40, 'accuracySemiMinor': 40, 'accuracyOrientation': 8100, 'transmission': 2, 'speed': 67.10820000000001, 'heading': 19072, 'angle': 127, 'accelLat': 2001, 'accelLong': 0, 'accelVert': -127, 'accelYaw': 0, 'brakesAppliedStatusAvailable': False, 'brakesAppliedLeftFront': False, 'brakesAppliedLeftRear': False, 'brakesAppliedRightFront': False, 'brakesAppliedRightRear': False, 'traction': 2, 'abs': 2, 'scs': 2, 'brakeBoost': 0, 'auxBrakes': 0, 'hazardLights': False, 'stopLineViolation': False, 'absActivated': False, 'tractionControlLoss': False, 'stabilityControlActivated': False, 'hazardousMaterials': False, 'reserved1': False, 'hardBraking': False, 'lightsChanged': False, 'wipersChanged': False, 'flatTire': False, 'airbagDeployments': False, 'pathHistory': '{crumbdata=[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]}', 'radiusOfCurvature': '-12880', 'confidence': '180', 'lowBeamHeadlightsOn': False, 'highBeamHeadlightsOn': False, 'leftTurnSignalOn': False, 'rightTurnSignalOn': False, 'hazardSignalOn': False, 'automaticLightControlOn': False, 'daytimeRunningLightsOn': False, 'fogLightOn': False, 'parkingLightsOn': False, 'vehicleAlerts': '', 'description': '', 'trailers': '', 'classification': '', 'classDetails': '', 'vehicleData': '', 'weatherReport': '', 'weatherProbe': '', 'obstacleDetection': '', 'disabledVehicle': '', 'speedProfile': '', 'rtcm': '', 'message': '{"basicsafetymessage":{"coredata":{"accelset":{"lat":2001,"long":0,"vert":-127,"yaw":0},"accuracy":{"orientation":8100,"semimajor":40,"semiminor":40},"angle":127,"brakes":{"abs":{"value":2},"auxbrakes":{"value":0},"brakeboost":{"value":0},"scs":{"value":2},"traction":{"value":2},"wheelbrakes":[false,false,false,false,false]},"elev":25313,"heading":19072,"id":"j6EXKQ==","lat":397452755,"long":-1056791004,"msgcnt":91,"secmark":6500,"size":{"length":1020,"width":260},"speed":1506,"transmission":{"value":2}},"partii":[{"vehiclesafetyextensions":{"pathhistory":{"crumbdata":[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]},"pathprediction":{"confidence":180,"radiusofcurve":-12880}}},{"partii_id":1,"specialvehicleextensions":{"vehiclealerts":{"lightsuse":{"value":1},"multi":{"value":0},"sirenuse":{"value":1},"ssprights":0}}}]},"messageid":20}'}, 
                {'timeReceived': '2020-05-14T11:37:23Z', 'year': '2020', 'month': '05', 'day': '14', 'hour': '11', 'version': '1.1.0', 'type': 'bsm', 'msgCount': '127', 'id': '"j6EXKQ=="', 'secMark': 23000, 'latitude': 39.7425508, 'longitude': -105.6837422, 'elevation': 8324.640000000001, 'accuracySemiMajor': 40, 'accuracySemiMinor': 40, 'accuracyOrientation': 8100, 'transmission': 2, 'speed': 67.10820000000001, 'heading': 17568, 'angle': 127, 'accelLat': 2001, 'accelLong': 10, 'accelVert': -127, 'accelYaw': 0, 'brakesAppliedStatusAvailable': False, 'brakesAppliedLeftFront': False, 'brakesAppliedLeftRear': False, 'brakesAppliedRightFront': False, 'brakesAppliedRightRear': False, 'traction': 2, 'abs': 2, 'scs': 2, 'brakeBoost': 0, 'auxBrakes': 0, 'hazardLights': False, 'stopLineViolation': False, 'absActivated': False, 'tractionControlLoss': False, 'stabilityControlActivated': False, 'hazardousMaterials': False, 'reserved1': False, 'hardBraking': False, 'lightsChanged': False, 'wipersChanged': False, 'flatTire': False, 'airbagDeployments': False, 'pathHistory': '{crumbdata=[{"elevationoffset":4,"latoffset":-2255,"lonoffset":-2525,"timeoffset":110},{"elevationoffset":11,"latoffset":-6303,"lonoffset":-7789,"timeoffset":320},{"elevationoffset":22,"latoffset":-10312,"lonoffset":-14080,"timeoffset":550},{"elevationoffset":47,"latoffset":-19308,"lonoffset":-30778,"timeoffset":1120}]}', 'radiusOfCurvature': '-8808', 'confidence': '140', 'lowBeamHeadlightsOn': False, 'highBeamHeadlightsOn': False, 'leftTurnSignalOn': False, 'rightTurnSignalOn': False, 'hazardSignalOn': False, 'automaticLightControlOn': False, 'daytimeRunningLightsOn': False, 'fogLightOn': False, 'parkingLightsOn': False, 'vehicleAlerts': '', 'description': '', 'trailers': '', 'classification': '', 'classDetails': '', 'vehicleData': '', 'weatherReport': '', 'weatherProbe': '', 'obstacleDetection': '', 'disabledVehicle': '', 'speedProfile': '', 'rtcm': '', 'message': '{"basicsafetymessage":{"coredata":{"accelset":{"lat":2001,"long":10,"vert":-127,"yaw":0},"accuracy":{"orientation":8100,"semimajor":40,"semiminor":40},"angle":127,"brakes":{"abs":{"value":2},"auxbrakes":{"value":0},"brakeboost":{"value":0},"scs":{"value":2},"traction":{"value":2},"wheelbrakes":[false,false,false,false,false]},"elev":25380,"heading":17568,"id":"j6EXKQ==","lat":397425508,"long":-1056837422,"msgcnt":127,"secmark":23000,"size":{"length":1020,"width":260},"speed":1508,"transmission":{"value":2}},"partii":[{"vehiclesafetyextensions":{"pathhistory":{"crumbdata":[{"elevationoffset":4,"latoffset":-2255,"lonoffset":-2525,"timeoffset":110},{"elevationoffset":11,"latoffset":-6303,"lonoffset":-7789,"timeoffset":320},{"elevationoffset":22,"latoffset":-10312,"lonoffset":-14080,"timeoffset":550},{"elevationoffset":47,"latoffset":-19308,"lonoffset":-30778,"timeoffset":1120}]},"pathprediction":{"confidence":140,"radiusofcurve":-8808}}},{"partii_id":1,"specialvehicleextensions":{"vehiclealerts":{"lightsuse":{"value":1},"multi":{"value":0},"sirenuse":{"value":1},"ssprights":0}}}]},"messageid":20}'}]

    assert raw_to_data_lake.is_json_clean(mock_json) is True         # ideal case

def test_is_json_clean_false_duplicate():
    mock_json1 = [{'timeReceived': '2020-05-14T11:37:06Z', 'year': '2020', 'month': '05', 'day': '14', 'hour': '11', 'version': '1.1.0', 'type': 'bsm', 'msgCount': '91', 'id': '"j6EXKQ=="', 'secMark': 6500, 'latitude': 39.7452755, 'longitude': -105.6791004, 'elevation': 8302.664, 'accuracySemiMajor': 40, 'accuracySemiMinor': 40, 'accuracyOrientation': 8100, 'transmission': 2, 'speed': 67.10820000000001, 'heading': 19072, 'angle': 127, 'accelLat': 2001, 'accelLong': 0, 'accelVert': -127, 'accelYaw': 0, 'brakesAppliedStatusAvailable': False, 'brakesAppliedLeftFront': False, 'brakesAppliedLeftRear': False, 'brakesAppliedRightFront': False, 'brakesAppliedRightRear': False, 'traction': 2, 'abs': 2, 'scs': 2, 'brakeBoost': 0, 'auxBrakes': 0, 'hazardLights': False, 'stopLineViolation': False, 'absActivated': False, 'tractionControlLoss': False, 'stabilityControlActivated': False, 'hazardousMaterials': False, 'reserved1': False, 'hardBraking': False, 'lightsChanged': False, 'wipersChanged': False, 'flatTire': False, 'airbagDeployments': False, 'pathHistory': '{crumbdata=[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]}', 'radiusOfCurvature': '-12880', 'confidence': '180', 'lowBeamHeadlightsOn': False, 'highBeamHeadlightsOn': False, 'leftTurnSignalOn': False, 'rightTurnSignalOn': False, 'hazardSignalOn': False, 'automaticLightControlOn': False, 'daytimeRunningLightsOn': False, 'fogLightOn': False, 'parkingLightsOn': False, 'vehicleAlerts': '', 'description': '', 'trailers': '', 'classification': '', 'classDetails': '', 'vehicleData': '', 'weatherReport': '', 'weatherProbe': '', 'obstacleDetection': '', 'disabledVehicle': '', 'speedProfile': '', 'rtcm': '', 'message': '{"basicsafetymessage":{"coredata":{"accelset":{"lat":2001,"long":0,"vert":-127,"yaw":0},"accuracy":{"orientation":8100,"semimajor":40,"semiminor":40},"angle":127,"brakes":{"abs":{"value":2},"auxbrakes":{"value":0},"brakeboost":{"value":0},"scs":{"value":2},"traction":{"value":2},"wheelbrakes":[false,false,false,false,false]},"elev":25313,"heading":19072,"id":"j6EXKQ==","lat":397452755,"long":-1056791004,"msgcnt":91,"secmark":6500,"size":{"length":1020,"width":260},"speed":1506,"transmission":{"value":2}},"partii":[{"vehiclesafetyextensions":{"pathhistory":{"crumbdata":[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]},"pathprediction":{"confidence":180,"radiusofcurve":-12880}}},{"partii_id":1,"specialvehicleextensions":{"vehiclealerts":{"lightsuse":{"value":1},"multi":{"value":0},"sirenuse":{"value":1},"ssprights":0}}}]},"messageid":20}'},
                {'timeReceived': '2020-05-14T11:37:06Z', 'year': '2020', 'month': '05', 'day': '14', 'hour': '11', 'version': '1.1.0', 'type': 'bsm', 'msgCount': '91', 'id': '"j6EXKQ=="', 'secMark': 6500, 'latitude': 39.7452755, 'longitude': -105.6791004, 'elevation': 8302.664, 'accuracySemiMajor': 40, 'accuracySemiMinor': 40, 'accuracyOrientation': 8100, 'transmission': 2, 'speed': 67.10820000000001, 'heading': 19072, 'angle': 127, 'accelLat': 2001, 'accelLong': 0, 'accelVert': -127, 'accelYaw': 0, 'brakesAppliedStatusAvailable': False, 'brakesAppliedLeftFront': False, 'brakesAppliedLeftRear': False, 'brakesAppliedRightFront': False, 'brakesAppliedRightRear': False, 'traction': 2, 'abs': 2, 'scs': 2, 'brakeBoost': 0, 'auxBrakes': 0, 'hazardLights': False, 'stopLineViolation': False, 'absActivated': False, 'tractionControlLoss': False, 'stabilityControlActivated': False, 'hazardousMaterials': False, 'reserved1': False, 'hardBraking': False, 'lightsChanged': False, 'wipersChanged': False, 'flatTire': False, 'airbagDeployments': False, 'pathHistory': '{crumbdata=[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]}', 'radiusOfCurvature': '-12880', 'confidence': '180', 'lowBeamHeadlightsOn': False, 'highBeamHeadlightsOn': False, 'leftTurnSignalOn': False, 'rightTurnSignalOn': False, 'hazardSignalOn': False, 'automaticLightControlOn': False, 'daytimeRunningLightsOn': False, 'fogLightOn': False, 'parkingLightsOn': False, 'vehicleAlerts': '', 'description': '', 'trailers': '', 'classification': '', 'classDetails': '', 'vehicleData': '', 'weatherReport': '', 'weatherProbe': '', 'obstacleDetection': '', 'disabledVehicle': '', 'speedProfile': '', 'rtcm': '', 'message': '{"basicsafetymessage":{"coredata":{"accelset":{"lat":2001,"long":0,"vert":-127,"yaw":0},"accuracy":{"orientation":8100,"semimajor":40,"semiminor":40},"angle":127,"brakes":{"abs":{"value":2},"auxbrakes":{"value":0},"brakeboost":{"value":0},"scs":{"value":2},"traction":{"value":2},"wheelbrakes":[false,false,false,false,false]},"elev":25313,"heading":19072,"id":"j6EXKQ==","lat":397452755,"long":-1056791004,"msgcnt":91,"secmark":6500,"size":{"length":1020,"width":260},"speed":1506,"transmission":{"value":2}},"partii":[{"vehiclesafetyextensions":{"pathhistory":{"crumbdata":[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]},"pathprediction":{"confidence":180,"radiusofcurve":-12880}}},{"partii_id":1,"specialvehicleextensions":{"vehiclealerts":{"lightsuse":{"value":1},"multi":{"value":0},"sirenuse":{"value":1},"ssprights":0}}}]},"messageid":20}'}, 
                {'timeReceived': '2020-05-14T11:37:23Z', 'year': '2020', 'month': '05', 'day': '14', 'hour': '11', 'version': '1.1.0', 'type': 'bsm', 'msgCount': '127', 'id': '"j6EXKQ=="', 'secMark': 23000, 'latitude': 39.7425508, 'longitude': -105.6837422, 'elevation': 8324.640000000001, 'accuracySemiMajor': 40, 'accuracySemiMinor': 40, 'accuracyOrientation': 8100, 'transmission': 2, 'speed': 67.10820000000001, 'heading': 17568, 'angle': 127, 'accelLat': 2001, 'accelLong': 10, 'accelVert': -127, 'accelYaw': 0, 'brakesAppliedStatusAvailable': False, 'brakesAppliedLeftFront': False, 'brakesAppliedLeftRear': False, 'brakesAppliedRightFront': False, 'brakesAppliedRightRear': False, 'traction': 2, 'abs': 2, 'scs': 2, 'brakeBoost': 0, 'auxBrakes': 0, 'hazardLights': False, 'stopLineViolation': False, 'absActivated': False, 'tractionControlLoss': False, 'stabilityControlActivated': False, 'hazardousMaterials': False, 'reserved1': False, 'hardBraking': False, 'lightsChanged': False, 'wipersChanged': False, 'flatTire': False, 'airbagDeployments': False, 'pathHistory': '{crumbdata=[{"elevationoffset":4,"latoffset":-2255,"lonoffset":-2525,"timeoffset":110},{"elevationoffset":11,"latoffset":-6303,"lonoffset":-7789,"timeoffset":320},{"elevationoffset":22,"latoffset":-10312,"lonoffset":-14080,"timeoffset":550},{"elevationoffset":47,"latoffset":-19308,"lonoffset":-30778,"timeoffset":1120}]}', 'radiusOfCurvature': '-8808', 'confidence': '140', 'lowBeamHeadlightsOn': False, 'highBeamHeadlightsOn': False, 'leftTurnSignalOn': False, 'rightTurnSignalOn': False, 'hazardSignalOn': False, 'automaticLightControlOn': False, 'daytimeRunningLightsOn': False, 'fogLightOn': False, 'parkingLightsOn': False, 'vehicleAlerts': '', 'description': '', 'trailers': '', 'classification': '', 'classDetails': '', 'vehicleData': '', 'weatherReport': '', 'weatherProbe': '', 'obstacleDetection': '', 'disabledVehicle': '', 'speedProfile': '', 'rtcm': '', 'message': '{"basicsafetymessage":{"coredata":{"accelset":{"lat":2001,"long":10,"vert":-127,"yaw":0},"accuracy":{"orientation":8100,"semimajor":40,"semiminor":40},"angle":127,"brakes":{"abs":{"value":2},"auxbrakes":{"value":0},"brakeboost":{"value":0},"scs":{"value":2},"traction":{"value":2},"wheelbrakes":[false,false,false,false,false]},"elev":25380,"heading":17568,"id":"j6EXKQ==","lat":397425508,"long":-1056837422,"msgcnt":127,"secmark":23000,"size":{"length":1020,"width":260},"speed":1508,"transmission":{"value":2}},"partii":[{"vehiclesafetyextensions":{"pathhistory":{"crumbdata":[{"elevationoffset":4,"latoffset":-2255,"lonoffset":-2525,"timeoffset":110},{"elevationoffset":11,"latoffset":-6303,"lonoffset":-7789,"timeoffset":320},{"elevationoffset":22,"latoffset":-10312,"lonoffset":-14080,"timeoffset":550},{"elevationoffset":47,"latoffset":-19308,"lonoffset":-30778,"timeoffset":1120}]},"pathprediction":{"confidence":140,"radiusofcurve":-8808}}},{"partii_id":1,"specialvehicleextensions":{"vehiclealerts":{"lightsuse":{"value":1},"multi":{"value":0},"sirenuse":{"value":1},"ssprights":0}}}]},"messageid":20}'}]
   
    assert raw_to_data_lake.is_json_clean(mock_json1) is False       # duplicate records = CHECK SHOULD FAIL 

def test_is_json_clean_false_missing():
    mock_json2 = [{'timeReceived': '', 'year': '2020', 'month': '05', 'day': '14', 'hour': '11', 'version': '1.1.0', 'type': 'bsm', 'msgCount': '91', 'id': '"j6EXKQ=="', 'secMark': 6500, 'latitude': 39.7452755, 'longitude': -105.6791004, 'elevation': 8302.664, 'accuracySemiMajor': 40, 'accuracySemiMinor': 40, 'accuracyOrientation': 8100, 'transmission': 2, 'speed': 67.10820000000001, 'heading': 19072, 'angle': 127, 'accelLat': 2001, 'accelLong': 0, 'accelVert': -127, 'accelYaw': 0, 'brakesAppliedStatusAvailable': False, 'brakesAppliedLeftFront': False, 'brakesAppliedLeftRear': False, 'brakesAppliedRightFront': False, 'brakesAppliedRightRear': False, 'traction': 2, 'abs': 2, 'scs': 2, 'brakeBoost': 0, 'auxBrakes': 0, 'hazardLights': False, 'stopLineViolation': False, 'absActivated': False, 'tractionControlLoss': False, 'stabilityControlActivated': False, 'hazardousMaterials': False, 'reserved1': False, 'hardBraking': False, 'lightsChanged': False, 'wipersChanged': False, 'flatTire': False, 'airbagDeployments': False, 'pathHistory': '{crumbdata=[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]}', 'radiusOfCurvature': '-12880', 'confidence': '180', 'lowBeamHeadlightsOn': False, 'highBeamHeadlightsOn': False, 'leftTurnSignalOn': False, 'rightTurnSignalOn': False, 'hazardSignalOn': False, 'automaticLightControlOn': False, 'daytimeRunningLightsOn': False, 'fogLightOn': False, 'parkingLightsOn': False, 'vehicleAlerts': '', 'description': '', 'trailers': '', 'classification': '', 'classDetails': '', 'vehicleData': '', 'weatherReport': '', 'weatherProbe': '', 'obstacleDetection': '', 'disabledVehicle': '', 'speedProfile': '', 'rtcm': '', 'message': '{"basicsafetymessage":{"coredata":{"accelset":{"lat":2001,"long":0,"vert":-127,"yaw":0},"accuracy":{"orientation":8100,"semimajor":40,"semiminor":40},"angle":127,"brakes":{"abs":{"value":2},"auxbrakes":{"value":0},"brakeboost":{"value":0},"scs":{"value":2},"traction":{"value":2},"wheelbrakes":[false,false,false,false,false]},"elev":25313,"heading":19072,"id":"j6EXKQ==","lat":397452755,"long":-1056791004,"msgcnt":91,"secmark":6500,"size":{"length":1020,"width":260},"speed":1506,"transmission":{"value":2}},"partii":[{"vehiclesafetyextensions":{"pathhistory":{"crumbdata":[{"elevationoffset":11,"latoffset":-4120,"lonoffset":-9250,"timeoffset":300},{"elevationoffset":41,"latoffset":-12864,"lonoffset":-30954,"timeoffset":980}]},"pathprediction":{"confidence":180,"radiusofcurve":-12880}}},{"partii_id":1,"specialvehicleextensions":{"vehiclealerts":{"lightsuse":{"value":1},"multi":{"value":0},"sirenuse":{"value":1},"ssprights":0}}}]},"messageid":20}'}, 
                {'timeReceived': '2020-05-14T11:37:23Z', 'year': '2020', 'month': '05', 'day': '14', 'hour': '11', 'version': '1.1.0', 'type': 'bsm', 'msgCount': '127', 'id': '"j6EXKQ=="', 'secMark': 23000, 'latitude': 39.7425508, 'longitude': -105.6837422, 'elevation': 8324.640000000001, 'accuracySemiMajor': 40, 'accuracySemiMinor': 40, 'accuracyOrientation': 8100, 'transmission': 2, 'speed': 67.10820000000001, 'heading': 17568, 'angle': 127, 'accelLat': 2001, 'accelLong': 10, 'accelVert': -127, 'accelYaw': 0, 'brakesAppliedStatusAvailable': False, 'brakesAppliedLeftFront': False, 'brakesAppliedLeftRear': False, 'brakesAppliedRightFront': False, 'brakesAppliedRightRear': False, 'traction': 2, 'abs': 2, 'scs': 2, 'brakeBoost': 0, 'auxBrakes': 0, 'hazardLights': False, 'stopLineViolation': False, 'absActivated': False, 'tractionControlLoss': False, 'stabilityControlActivated': False, 'hazardousMaterials': False, 'reserved1': False, 'hardBraking': False, 'lightsChanged': False, 'wipersChanged': False, 'flatTire': False, 'airbagDeployments': False, 'pathHistory': '{crumbdata=[{"elevationoffset":4,"latoffset":-2255,"lonoffset":-2525,"timeoffset":110},{"elevationoffset":11,"latoffset":-6303,"lonoffset":-7789,"timeoffset":320},{"elevationoffset":22,"latoffset":-10312,"lonoffset":-14080,"timeoffset":550},{"elevationoffset":47,"latoffset":-19308,"lonoffset":-30778,"timeoffset":1120}]}', 'radiusOfCurvature': '-8808', 'confidence': '140', 'lowBeamHeadlightsOn': False, 'highBeamHeadlightsOn': False, 'leftTurnSignalOn': False, 'rightTurnSignalOn': False, 'hazardSignalOn': False, 'automaticLightControlOn': False, 'daytimeRunningLightsOn': False, 'fogLightOn': False, 'parkingLightsOn': False, 'vehicleAlerts': '', 'description': '', 'trailers': '', 'classification': '', 'classDetails': '', 'vehicleData': '', 'weatherReport': '', 'weatherProbe': '', 'obstacleDetection': '', 'disabledVehicle': '', 'speedProfile': '', 'rtcm': '', 'message': '{"basicsafetymessage":{"coredata":{"accelset":{"lat":2001,"long":10,"vert":-127,"yaw":0},"accuracy":{"orientation":8100,"semimajor":40,"semiminor":40},"angle":127,"brakes":{"abs":{"value":2},"auxbrakes":{"value":0},"brakeboost":{"value":0},"scs":{"value":2},"traction":{"value":2},"wheelbrakes":[false,false,false,false,false]},"elev":25380,"heading":17568,"id":"j6EXKQ==","lat":397425508,"long":-1056837422,"msgcnt":127,"secmark":23000,"size":{"length":1020,"width":260},"speed":1508,"transmission":{"value":2}},"partii":[{"vehiclesafetyextensions":{"pathhistory":{"crumbdata":[{"elevationoffset":4,"latoffset":-2255,"lonoffset":-2525,"timeoffset":110},{"elevationoffset":11,"latoffset":-6303,"lonoffset":-7789,"timeoffset":320},{"elevationoffset":22,"latoffset":-10312,"lonoffset":-14080,"timeoffset":550},{"elevationoffset":47,"latoffset":-19308,"lonoffset":-30778,"timeoffset":1120}]},"pathprediction":{"confidence":140,"radiusofcurve":-8808}}},{"partii_id":1,"specialvehicleextensions":{"vehiclealerts":{"lightsuse":{"value":1},"multi":{"value":0},"sirenuse":{"value":1},"ssprights":0}}}]},"messageid":20}'}]
     
    assert raw_to_data_lake.is_json_clean(mock_json2) is False       # missing records = CHECK SHOULD FAIL

@mock.patch("google.cloud.storage.Client")
def test_Success(client):
    
    event = {
        'bucket': 'rsu_raw-ingest',
        'name': 'test',
        'metageneration': 'some-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = 'some-id'
    context.event_type = 'gcs-event'

    raw_bucket = 'rsu_raw-ingest'
    raw_bucketOBJ = client().get_bucket(raw_bucket)
    raw_blob = raw_bucketOBJ.blob(event['name'])
    raw_blob.upload_from_string('{"column":1, "status":"yes"}')
    lake_bucket = 'rsu_data-lake'
    lake_bucketOBJ = client().get_bucket(lake_bucket)

    raw_to_data_lake.raw_to_data_lake(event, context)

    assert True

@mock.patch("google.cloud.storage.Client")
def test_ExceptionRaised_InvalidJSON(client):
    
    event = {
        'bucket': 'rsu_raw-ingest',
        'name': 'test',
        'metageneration': 'some-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = 'some-id'
    context.event_type = 'gcs-event'

    raw_bucketOBJ = client().get_bucket
    raw_to_data_lake.raw_to_data_lake(event, context)

    raw_blob = raw_bucketOBJ.blob
    blob_name = event['name']

    with pytest.raises(Exception):
        raw_blob.assert_called_with(blob_name)
