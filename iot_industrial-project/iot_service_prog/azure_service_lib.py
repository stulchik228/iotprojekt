import asyncio
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import Twin, TwinProperties, CloudToDeviceMethod
from azure.storage.blob import BlobServiceClient
import json


async def receive_twin_reported(manager_client, device_id):

    twin = manager_client.get_twin(device_id)
    rep = twin.properties.reported
    print("Twin reported:")
    print(rep)
    return rep


async def twin_desired(manager_client, device_id, reported):

    desired_twin = {}

    del reported["$metadata"]
    del reported["$version"]

    for key, value in reported.items():
        desired_twin[key] = {"ProductionRate": value["ProductionRate"]}

    twin = manager_client.get_twin(device_id)
    twin_patch = Twin(properties=TwinProperties(desired=desired_twin))
    twin = manager_client.update_twin(device_id, twin_patch, twin.etag)




async def clear_desired_twin(manager, device_id):

    twin = manager.get_twin(device_id)
    des = twin.properties.desired
    del des["$metadata"]
    del des["$version"]
    for key, value in des.items():
        des[key] = None

    twin_patch = Twin(properties=TwinProperties(desired=des))
    twin = manager.update_twin(device_id, twin_patch, twin.etag)




async def clear_blob_storage(connection_str):

    blob_dev_err_name = 'device-err'
    blob_temperature_name = 'kpi-production'
    blob_kpi_name = 'temperature-info'

    blob_service = BlobServiceClient.from_connection_string(connection_str)



    try:
        blob_service.delete_container(blob_dev_err_name)
    except:
        pass



    try:
        blob_service.delete_container(blob_temperature_name)
    except:
        pass



    try:
        blob_service.delete_container(blob_kpi_name)
    except:
        pass


    print("Blobs are cleared")



async def run_emergency_stop(manager, dev_name):

    cd = CloudToDeviceMethod(method_name="emergency_stop", payload={"DeviceName": dev_name})
    manager.invoke_device_method("test_device", cd)



async def run_res_err_status(manager, dev_name):

    cd = CloudToDeviceMethod(method_name="reset_err_status", payload={"DeviceName": dev_name})
    manager.invoke_device_method("test_device", cd)



async def read_blobs(manager, device_id, connection_str, date_err, date_kpi):

    blob_dev_err_name = 'device-err'
    blob_temperature_name = 'temperature-info'
    blob_kpi_name = 'kpi-production'



    blob_service_client = BlobServiceClient.from_connection_string(connection_str)



    ret_date_err = date_err
    ret_date_kpi = date_kpi



    # Работа с ошибками устройства
    try:
        dev_err_container_client = blob_service_client.get_container_client(blob_dev_err_name)


        lst_dev_err_json = []


        for blob in dev_err_container_client.list_blobs():
            data = dev_err_container_client.download_blob(blob).readall()
            data = data.decode("utf-8")
            lst_dev_err_json.append(data.split('\r\n'))


        lst_dic_dev_err = []


        for i in range(len(lst_dev_err_json)):
            for j in range(len(lst_dev_err_json[i])):
                lst_dic_dev_err.append(json.loads(lst_dev_err_json[i][j]))


        print(f"ERR JSON: " + str(lst_dev_err_json))
        print(f"DIC ERR: " + str(lst_dic_dev_err))


        for i in range(len(lst_dic_dev_err)):
            if lst_dic_dev_err[i]["windowEndTime"] > date_err:
                ret_date_err = lst_dic_dev_err[i]["windowEndTime"]
                device_name = lst_dic_dev_err[i]["DeviceName"]
                await run_emergency_stop(manager, device_name)

    except:
        pass



    # Работа с температурой
    try:
        temperature_container_client = blob_service_client.get_container_client(blob_temperature_name)


        lst_temp_json = []


        for blob in temperature_container_client.list_blobs():
            data = temperature_container_client.download_blob(blob).readall()
            data = data.decode("utf-8")
            lst_temp_json.append(data.split('\r\n'))


        lst_dic_temp = []


        for i in range(len(lst_temp_json)):
            for j in range(len(lst_temp_json[i])):
                lst_dic_temp.append(json.loads(lst_temp_json[i][j]))



        print(f"TEMP JSON: " + str(lst_temp_json))
        print(f"DIC TEMP: " + str(lst_dic_temp))
    except:
        pass



    # Работа с KPI
    try:
        kpi_container_client = blob_service_client.get_container_client(blob_kpi_name)

        lst_kpi_json = []

        for blob in kpi_container_client.list_blobs():
            data = kpi_container_client.download_blob(blob).readall()
            data = data.decode("utf-8")
            lst_kpi_json.append(data.split('\r\n'))


        lst_dic_kpi = []


        for i in range(len(lst_kpi_json)):
            for j in range(len(lst_kpi_json[i])):
                lst_dic_kpi.append(json.loads(lst_kpi_json[i][j]))

        print(f"KPI JSON: " + str(lst_kpi_json))
        print(f"DIC KPI: " + str(lst_dic_kpi))



        for i in range(len(lst_dic_kpi)):
            if lst_dic_kpi[i]["windEndTime"] > date_kpi:

                ret_date_kpi = lst_dic_kpi[i]["windEndTime"]


                if lst_dic_kpi[i]["KPI"] < 90:
                    print("---------kpi------------")
                    twin = manager.get_twin(device_id)
                    des = twin.properties.desired


                    dev_name = "Device" + str(lst_dic_kpi[i]["DeviceName"])[-1]
                    prod_rate = des[dev_name]["ProductionRate"] - 10

                    update_tw = {dev_name: {"ProductionRate": prod_rate}}



                    twin_patch = Twin(properties=TwinProperties(desired=update_tw))
                    twin = manager.update_twin(device_id, twin_patch, twin.etag)


    except:
        pass

    return ret_date_err, ret_date_kpi

