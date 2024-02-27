import time
from azure.iot.device import IoTHubDeviceClient, Message, IoTHubModuleClient, MethodResponse, MethodRequest
import asyncio
import json
from azure.communication.email import EmailClient
from azure.core.credentials import AzureKeyCredential


async def d2c(client, machine, dev_err=False):

    message = {}
    message["DeviceName"] = str(machine.node)[7:]
    message["ProductionStatus"] = machine.production_status
    message["WorkorderId"] = machine.workorder_id
    message["ProductionRate"] = machine.production_rate
    message["GoodCount"] = machine.good_count
    message["BadCount"] = machine.bad_count
    message["Temperature"] = machine.temperature
    message["DeviceError"] = machine.device_error

    if dev_err:
        message["IsDevErr"] = "true"
    else:
        message["IsDevErr"] = "false"

    client.send_message(str(message))


async def twin_reported(client, machine):

    reported_props = {"Device" + str(machine.node)[-1]: {"ProductionRate": machine.production_rate,
                                                            "Errors": machine.device_error}}
    client.patch_twin_reported_properties(reported_props)


async def compare_production_rates(twin_patch, lst_devices):

    for i in range(len(lst_devices)):
        name = "Device" + str(lst_devices[i].node)[-1]
        if name in twin_patch.keys() and twin_patch[name] is not None and twin_patch[name]["ProductionRate"] != lst_devices[i].production_rate:
            await lst_devices[i].set_prod_rate()
            print(f"{name} set production rate successfully")


async def receive_twin_desired(client, lst_devices):

    def twin_patch_handler(twin_patch):
        try:
            print("Twin patch received")
            print(twin_patch)
            asyncio.run(compare_production_rates(twin_patch, lst_devices))
        except Exception as e:
            print(f"Exception: {str(e)}")


    try:
        client.on_twin_desired_properties_patch_received = twin_patch_handler
    except Exception as e:
        print(f"Exception: {str(e)}")


async def run_emergency_stop(opc_client, device_name):


    nodeES = opc_client.get_node(f"ns=2;s={device_name}/EmergencyStop")
    node = opc_client.get_node(f"ns=2;s={device_name}")
    await node.call_method(nodeES)
    print("Emergency stop called. Success")


async def run_res_err_status(opc_client, device_name):


    nodeRES = opc_client.get_node(f"ns=2;s={device_name}/ResetErrorStatus")
    node = opc_client.get_node(f"ns=2;s={device_name}")
    await node.call_method(nodeRES)
    print("Reset error status called. Success")


async def take_direct_method(client, opc_client):


    def handle_method(request):
        try:
            print(f"Direct Method called: {request.name}")
            print(f"Request: {request}")
            print(f"Payload: {request.payload}")

            if request.name == "emergency_stop":
                device_name = request.payload["DeviceName"]
                asyncio.run(run_emergency_stop(opc_client, device_name))

            elif request.name == "reset_err_status":
                device_name = request.payload["DeviceName"]
                asyncio.run(run_res_err_status(opc_client, device_name))

            response_payload = "Method executed successfully"
            response = MethodResponse.create_from_method_request(request, 200, payload=response_payload)
            print(f"Response: {response}")
            print(f"Payload: {response.payload}")
            client.send_method_response(response)
            return response
        except Exception as e:
            print(f"Exception caught in handle_method: {str(e)}")

    try:
        client.on_method_request_received = handle_method
    except:
        pass


async def send_email(device, err_lst):


    credential = AzureKeyCredential("42z6cG8xhlKogXLGGHcRk8heBZoJqW/y0jSztdB4MkvYxOCTe9U45Zd1p380ai5pfJyuP7y1jt3EMZM9YL+Rlw==")
    endpoint = "https://iot-send-emails.europe.communication.azure.com/"
    email_client = EmailClient(endpoint, credential)

    message = {
        "senderAddress": "DoNotReply@ae7eedae-c1dd-499e-9863-ff0f19ab520f.azurecomm.net",
        "recipients": {
            "to": [{"address": "kirill2446@gmail.com"}],
        },
        "content": {
            "subject": "Error occurrence",
            "plainText": f"{str(device.node)[7:]} have a new error: " + ", ".join(err_lst),
        }
    }

    poller = email_client.begin_send(message)
    result = poller.result()
    print(f"Send email. {str(device.node)[7:]} have new error.")
    print(f"Result sending email: {result}")


def device_errors_compare(lst_old, lst_new):


    flag = False
    errs = []
    switch_dict = [
        "Unknown",
        "Sensor Failure",
        "Power Failure",
        "Emergency Stop"
    ]

    for i in range(len(lst_old)):
        if lst_old[i] == 0 and lst_new[i] == 1:
            flag = True
            errs.append(switch_dict[i])

    return flag, errs

