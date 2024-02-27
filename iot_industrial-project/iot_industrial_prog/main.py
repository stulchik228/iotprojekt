import time
import sys
from machine_lib import Machine, asyncio, ua
from asyncua import Client
from azure_device_lib import \
    IoTHubModuleClient, \
    Message, \
    d2c, \
    twin_reported, \
    receive_twin_desired, \
    take_direct_method, \
    send_email, \
    device_errors_compare


async def main():
    opcua_endpoint = "opc.tcp://localhost:4840/"
    CONNECTION_STRING = "HostName=IOT-projekt-Biaheza.azure-devices.net;DeviceId=Device1;SharedAccessKey=WPdl4W8Q5XVCVqBgtsQEtj6NKyRX1VvKULiaYkje53k="

    client_opc = Client(opcua_endpoint)



    # Подключение к серверу OPC UA
    try:
        await client_opc.connect()
    except Exception as e:
        print("Not connect to OPC UA")
        print(f"Error: {e}")
        sys.exit(1)
    else:
        print("Successful connection to OPC UA")




    # Подключение к IoTHub
    try:
        client_iot = IoTHubModuleClient.create_from_connection_string(CONNECTION_STRING)
        client_iot.connect()
    except Exception as e:
        print("Not connect to IoTHub")
        print(f"Error: {e}")
        sys.exit(1)
    else:
        print("Successful connection to IoTHub")




    # Очистка отчетного двойника
    twin = client_iot.get_twin()['reported']
    del twin["$version"]
    for key, value in twin.items():
        twin[key] = None
    client_iot.patch_twin_reported_properties(twin)

    lst_dev_err_old = []




    try:
        while True:
            lst = await client_opc.get_objects_node().get_children()
            lst = lst[1:]

            lst_machines = []

            lst_dev_err_new = []

            for i in range(len(lst)):
                machine = Machine(client_opc, lst[i])
                await machine.update_data()
                lst_machines.append(machine)

                lst_dev_err_new.append(machine.device_error)

            await receive_twin_desired(client_iot, lst_machines)



            # вывод актуальных данных о наших устройствах
            for j in range(len(lst_machines)):
                # print(lst_machines[j])
                await d2c(client_iot, lst_machines[j])
                await twin_reported(client_iot, lst_machines[j])



                # при изменении значения должно быть отправлено единичное сообщение D2C на платформу IoT
                if lst_dev_err_old != []:
                    res_comp = device_errors_compare(lst_dev_err_old[j], lst_dev_err_new[j])
                    if res_comp[0]:
                        await d2c(client_iot, lst_machines[j], True)



                        # отправка электронной почты на предопределенный адрес
                        await send_email(lst_machines[j], res_comp[1])



            # take a direct methods and call them
            await take_direct_method(client_iot, client_opc)



            # актуализация
            lst_dev_err_old = []
            for err in lst_dev_err_new:
                lst_dev_err_old.append(err)



            # опционально
            time.sleep(1)
    except KeyboardInterrupt:
        print("Keyboard stopped program")




    # отключение
    await client_opc.disconnect()
    client_iot.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
