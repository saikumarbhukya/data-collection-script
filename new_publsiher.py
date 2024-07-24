import json
import os
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from threading import Thread
from multiprocessing import Process
from urllib.parse import quote_plus
import pytz
import cv2
import neoapi
import pika
import requests
import numpy as np
from uuid import uuid4
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from platform_configuration import constants_platform as constants
from platform_connectivity.utility import Utility

connect = Utility(configuration=constants.camera_publisher_configuration,
                  rabbitmq_publisher=constants.camera_publisher_rabbitmq_publisher)

############################ Image Saving params #########################
save_area_name = ["RMHS Setup01","RMHS Setup02","RMHS Setup03"]
#save_area_name = ["Blast Furnace Setup02"]
num_imgs_coll = {area: 0 for area in save_area_name}
# prefix = f"/ext512/JSW/nvidia/record_imgs/test_{'_'.join(time.asctime().split(' ')[1:]).replace(':', '_')}"

##########################################################################


# TODO: Add to fetch_camera_configuration()
with open('scripts/camera_config.json', 'r') as f:
    JSW_Camera_dict = json.load(f)
tz_NY = pytz.timezone('Asia/Kolkata')

def compress_image(image_data, max_width, max_height, quality):
    with Image.open(io.BytesIO(image_data)) as image:
        image.thumbnail((max_width, max_height))
        with io.BytesIO() as output:
            image.save(output, format='JPEG', quality=quality)
            return output.getvalue()

def fetch_camera_configuration(count=0):
    try:
        response = connect.master_redis.get_val(key="camera_configuration")
        if not response:
            connect.loginfo("fetching configuration from {}".format(os.path.join(
                connect.config_json.get("app_server_host") + "fetch_cameras_details")))
            response = requests.get(os.path.join(connect.config_json.get("app_server_host") + "fetch_cameras_details"), headers={
                                    "content-type": "text", "entity-location": json.dumps(connect.config_json.get("entity-location"))})
            connect.loginfo("fetched data " + str(response.content))
            response = json.loads(response.content)
            connect.loginfo("configuration fetched {}".format(str(response)))
            connect.config_json["camera_config"] = response["camera_config"]
            connect.config_json["subscriber_config"] = response["subscriber_config"]
            connect.config_json["model_details"] = response["model_details"]
            connect.config_json["use_case_list"] = response["use_case_list"]
            connect.master_redis.set_val(
                key="camera_configuration", val=json.dumps(response))
        else:
            response = json.loads(response)
            connect.config_json["camera_config"] = response["camera_config"]
            connect.config_json["subscriber_config"] = response["subscriber_config"]
            connect.config_json["model_details"] = response["model_details"]
            connect.config_json["use_case_list"] = response["use_case_list"]
    except Exception as e_:
        if count == 0:
            fetch_camera_configuration(count=count + 1)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        connect.loginfo("Exception occurred in fetch_camera_configuration : " +
                        str(e_) + ' ' + str(exc_tb.tb_lineno))


class CameraManager:
    def __init__(self):
        """
        Heart-beat monitor to do the following:
        *   Check active camera threads
        *   Check publish status of camera threads
        *   Initialize and Terminate camera threads
            based on Redis Config update
        """
        try:
            # fetch_camera_configuration()
            # self.cam_dict = connect.config_json["camera_config"]
            self.cam_dict = JSW_Camera_dict["camera_info"]
            if "NoOfCameras" in self.cam_dict:
                self.cam_dict.pop("NoOfCameras")

            connect.loginfo('cam_dict keys: {}'.format(
                str(self.cam_dict.keys())), 'info')

            # Process Status config
            self.stat_conn = Utility(configuration=constants.status_publisher_configuration,
                  rabbitmq_publisher=constants.status_publisher_rabbitmq_publisher)
            connect.loginfo(f"###########################################################Process Status Config {self.stat_conn}")
            self.proc_stat = {True: "<{}, {}> is on", False: "<{}, {}> is not running"}
            self.color = {True: "G", False: "R"}
            self.time_format = '%Y-%m-%d %H:%M:%S'

            # Camera Thread List
            self.streams = {}
            for key in self.cam_dict.keys():
                self.streams[key] = {}
                self.streams[key] = JSWCameraClass(
                    setup_name=key, param_dict=self.cam_dict[key])
                connect.loginfo(f"#############Streams Dictionary {self.streams}")
                connect.loginfo(
                    "[CameraManager] Thread Object initialized for setup {}".format(key))

            for key in self.streams.keys():
                self.streams[key].start()
                connect.loginfo(
                    "[CameraManager] Thread started for setup {}".format(key))
        except Exception as e_:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            connect.loginfo("[HeartBeatMonitor] Exception occurred in HeartBeatMonitor __init__ : " +
                            str(e_) + ' - line no.: ' + str(exc_tb.tb_lineno), level='error')

    def threadCheck(self):
        """
        *   Check Camera health
        *   Update camera threadlist based on updated config
        """
        while True:
            try:
                time.sleep(1.0)
                kill_loop = False
                # Check camera health
                connect.loginfo('[HeartBeatMonitor] Checking Camera Health')
                for key in self.streams.keys():
                    connect.loginfo(self.streams[key])
                    connect.loginfo(
                        f'[{self.streams[key].name}]: {self.streams[key].cam_health}')

                    packet = {
                        'message': self.proc_stat[self.streams[key].cam_health].format(
                            self.streams[key].name, self.streams[key].cam_left_ip),
                        'publisher': "camera_publisher_process_check",
                        'color': self.color[self.streams[key].cam_health],
                        'datetime': datetime.strftime(datetime.now(), self.time_format)
                    }
                    connect.loginfo("Publishing packet to Task: {}".format(packet))
                    self.stat_conn.publish(event=json.dumps(
                            packet), routing_key='backend_process_check')

                    if not self.streams[key].cam_health:
                        self.streams[key].stop()
                        # kill_loop = True
                        connect.loginfo(f"Reinitializing {self.streams[key]} ...")
                        time.sleep(0.5)
                        self.streams[key] = JSWCameraClass(
                            setup_name=key, param_dict=self.cam_dict[key])
                        self.streams[key].start()
                        connect.loginfo(
                            "[CameraManager] Thread started for setup {}".format(key))
                if kill_loop:
                    self.stop()
                    connect.loginfo("Publisher shutting down for reinitialization ...")
                    break
            except Exception as e_:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                connect.loginfo("[HeartBeatMonitor] Exception occurred in threadCheck : " +
                                e_.__str__() + ' - line no.: ' + str(exc_tb.tb_lineno), level='error')

    def stop(self):
        """
        Releasing all camera streams.
        """
        try:
            for key in self.streams.keys():
                self.streams[key].stop()
                # self.streams[key].release_cam()
        except Exception as e_:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            connect.loginfo("[HeartBeatMonitor] Exception occurred in stop : " +
                            e_.__str__() + ' - line no.: ' + str(exc_tb.tb_lineno), level='error')


class MasterCameraClass(ABC, Thread):
    def __init__(self):
        Thread.__init__(self)
        self.name = None
        self.routing_key = None
        self.frame_rate = None
        self.kill_flag = False
        self.connect = Utility(configuration=constants.camera_publisher_configuration,
                               rabbitmq_publisher=constants.camera_publisher_rabbitmq_publisher)

    def run(self):
        try:
            prev = 0
            while True:
                if self.kill_flag:
                    connect.loginfo(f'[Camera] [{self.name}] kill_flag={self.kill_flag} Camera killed.')
                    break
                # connect.loginfo(
                #     f'[Camera] [{self.name}] [kill_flag={self.kill_flag}] kill_flag check')
                time_elapsed = time.time() - prev
                if time_elapsed > 1.0 / float(self.frame_rate):
                    prev = time.time()
                    packet = self.acqFrames()
                    if packet is not None:
                        self.connect.publish(event=json.dumps(
                            packet), routing_key=self.routing_key)
                        connect.loginfo(
                            f'[Camera] [{self.name}] Packet published')
                    else:
                        connect.loginfo(
                            '[{}] - Camera thread not returning frames.'.format(self.name), level='error')
            connect.loginfo(f'[Camera] [{self.name}] kill_flag={self.kill_flag} out of loop')
            try:
                if self.Cam_left.IsConnected():
                    self.Cam_left.Disconnect()
                if self.Cam_right.IsConnected():
                    self.Cam_right.Disconnect()
            except Exception as err:
                connect.loginfo(f"[Camera] [{self.name}] camera disconnect not valid.", level='error')
            # self.restart_camera()
        except Exception as e_:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            # self.restart_camera() # restarting camera on exception
            connect.loginfo(f"[{self.name}] Exception occurred in update {exc_type}: " +
                            str(e_) + ' ' + str(exc_tb.tb_lineno), level='error')
            self.stop()

    def stop(self):
        connect.loginfo(f"[Camera] [{self.name}] Stopping Camera.")
        self.kill_flag = True

    @abstractmethod
    def acqFrames(self):
        """
        Must return data packet
        """
        raise NotImplementedError

    @abstractmethod
    def restart_camera(self):
        """
        Must restart camera.
        """
        raise NotImplementedError

    @abstractmethod
    def init_camera(self):
        """
        Must Initialize cameras
        """
        raise NotImplementedError


class JSWCameraClass(MasterCameraClass):
    def __init__(self, setup_name='', param_dict=''):
        try:
            super(JSWCameraClass, self).__init__()
            self.name = setup_name
            self.param_dict = param_dict
            self.area = self.param_dict['area']
            cam_name_list = [self.name + '_Cam01', self.name + '_Cam02']
            self.routing_key = '.'.join(
                cam_name_list + list(self.param_dict["model_list"].keys()))
            self.model_list = self.param_dict["model_list"]
            self.cam_health = True
            self.time_format_1 = "%Y-%m-%d"
            self.cam_left_ip = self.param_dict['camera_left_ip']
            self.cam_right_ip = self.param_dict['camera_right_ip']
            self.exposure = self.param_dict['exposure']
            self.exposure_right = self.param_dict['exposure_right']
            self.gain_right = self.param_dict['gain_right']
            self.gain = self.param_dict['gain']
            self.pixel_format = 'BGR8'
            self.frame_rate = self.param_dict['frame_rate']
            self.encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
            self.time_format = '%Y-%m-%d %H:%M:%S'
            self.image_expiry_time = 20
            self.init_camera()

        except Exception as err:
            self.cam_health = False
            exc_type, exc_obj, exc_tb = sys.exc_info()
            connect.loginfo(
                f'[Camera] [{self.name}] Error in init method: {err.__str__()} - line no. : {exc_tb.tb_lineno}', level='error')

    def acqFrames(self):
        try:
            self.Cam_left.f.TriggerSoftware.Execute()
            self.Cam_right.f.TriggerSoftware.Execute()
            img_left = self.Cam_left.GetImage(200).GetNPArray()
            img_right = self.Cam_right.GetImage(200).GetNPArray()

            ######## Recording_Images ###################
            temp = connect.master_redis.get_val(key="start_time")
            try:
                counter = abs(((datetime.strptime((str(datetime.now())).split('.')[0], self.time_format)) - datetime.strptime(temp, self.time_format)).total_seconds())
                key_existence = connect.master_redis.key_exist("start_time")
                self.connect.loginfo(f"Time Started in Secs {counter}")
            except:
                pass

            try:
                if connect.master_redis.get_val(key='record') == 'true' and (counter < 1200) and (key_existence == True):
                    prefix = '/ext512/JSW/nvidia/record_imgs/record_pub_' + datetime.strftime(datetime.now(tz_NY), self.time_format_1)
                    area = self.area
                    img = cv2.hconcat([img_left, img_right])
                    save_pub_img = img
                    if not os.path.isdir(prefix):
                        os.mkdir(prefix)
                    if connect.master_redis.get_val(key=area) == 'true':
                        if not os.path.isdir(os.path.join(prefix, area)):
                            os.mkdir(os.path.join(prefix, area))
                        img_name = f"{area}_{num_imgs_coll[area]}.jpg"
                        cv2.imwrite(os.path.join(prefix, area, img_name), save_pub_img)
                        num_imgs_coll[area] += 1
                        self.connect.loginfo("[{}] Image saved:: {}".format(self.name, img_name))
                else:
                    try:
                        connect.master_redis.del_key(keys=["start_time"])
                        self.connect.loginfo("Recording stopped")
                    except:
                        pass
            except:
                pass

            enc_time = time.time()
            _, enc_left = cv2.imencode('.jpg', img_left, self.encode_param)
            _, enc_right = cv2.imencode('.jpg', img_right, self.encode_param)
            uid_left = str(uuid4())
            uid_right = str(uuid4())
            compressed_left = compress_image(enc_left.tobytes(), max_width=2048, max_height=390, quality=50)
            compressed_right = compress_image(enc_right.tobytes(), max_width=2048, max_height=390, quality=50)
            self.connect.master_redis.set_val(key=uid_left, val=compressed_left.hex(), ex=self.image_expiry_time)
            self.connect.master_redis.set_val(key=uid_right, val=compressed_right.hex(), ex=self.image_expiry_time)

            self.connect.loginfo(f"Encoded Time {time.time() - enc_time}")

            res_packet = {
                "camera": self.name,
                "camera_ip": self.param_dict['camera_ip'],
                "rtsp": "dummy_rtsp",
                "location": self.param_dict['location'],
                "area": self.area,
                "usecase": self.param_dict['usecase'],
                "model_list": self.model_list,
                "datetime": datetime.strftime(datetime.now(tz_NY), self.time_format),
                "width": self.param_dict['width'],
                "height": self.param_dict['height'],
                "uuid": {'uid_left': uid_left, 'uid_right': uid_right},
                "image": {'left_camera': "", 'right_camera': ""}
            }
            self.cam_health = True
            connect.loginfo(f'[Camera] [{self.name}] packet created')
            return res_packet
        except Exception as err:
            self.cam_health = False
            exc_type, exc_obj, exc_tb = sys.exc_info()
            connect.loginfo(
                f'[Camera] [{self.name}] Error in acqFrames method: {err.__str__()} - line no. : {exc_tb.tb_lineno}', level='error')

    def restart_camera(self):
        pass

    def init_camera(self):
        try:
            self.Cam_left = neoapi.Cam()
            self.Cam_right = neoapi.Cam()

            self.Cam_left.Connect(self.cam_left_ip)
            self.Cam_right.Connect(self.cam_right_ip)

            ################ Binning #################
            self.Cam_left.f.BinningSelector.SetString("Region0")
            self.Cam_left.f.BinningHorizontal.Set(2)
            self.Cam_left.f.BinningVertical.Set(2)

            self.Cam_right.f.BinningSelector.SetString("Region0")
            self.Cam_right.f.BinningHorizontal.Set(2)
            self.Cam_right.f.BinningVertical.Set(2)

            ############### Cropping ##################
            self.Cam_left.f.Width.Set(self.param_dict['width'])
            self.Cam_left.f.Height.Set(self.param_dict['height'])
            self.Cam_left.f.OffsetX.Set(self.param_dict['left_offsetX'])
            self.Cam_left.f.OffsetY.Set(self.param_dict['left_offsetY'])

            self.Cam_right.f.Width.Set(self.param_dict['width'])
            self.Cam_right.f.Height.Set(self.param_dict['height'])
            self.Cam_right.f.OffsetX.Set(self.param_dict['right_offsetX'])
            self.Cam_right.f.OffsetY.Set(self.param_dict['right_offsetY'])

            self.Cam_left.f.TriggerMode.SetString("On")
            self.Cam_right.f.TriggerMode.SetString("On")

            self.Cam_left.f.ExposureTime.Set(self.exposure)
            self.Cam_right.f.ExposureTime.Set(self.exposure_right)

            self.Cam_left.f.Gain.Set(self.gain)
            self.Cam_right.f.Gain.Set(self.gain_right)
            self.Cam_left.f.PixelFormat.SetString(self.pixel_format)
            self.Cam_right.f.PixelFormat.SetString(self.pixel_format)

            self.Cam_left.f.TriggerSource.SetString("Software")
            self.Cam_right.f.TriggerSource.SetString("Software")

            self.Cam_left.f.GevSCPD.Set(
                ((6-1) * self.Cam_left.f.GevSCPSPacketSize.Get() * 8) * 110 // 100)
            self.Cam_right.f.GevSCPD.Set(
                ((6-1) * self.Cam_right.f.GevSCPSPacketSize.Get() * 8) * 110 // 100)

            self.Cam_left.ClearImages()
            self.Cam_right.ClearImages()
        except Exception as err:
            self.cam_health = False
            exc_type, exc_obj, exc_tb = sys.exc_info()
            connect.loginfo(
                f'[Camera] [{self.name}] Error in init_camera method: {err.__str__()} - line no. : {exc_tb.tb_lineno}', level='error')


####################################################################################################################################################
if __name__ == '__main__':
    try:
        hbeat = CameraManager()
        hbeat.threadCheck()
    except Exception as e_:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        connect.loginfo("[Main] Exception occurred in main : " +
                        str(e_) + ' line no.: ' + str(exc_tb.tb_lineno), level='error')