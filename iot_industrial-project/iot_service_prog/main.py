import asyncio
import sys
import time
from azure_service_lib import \
    IoTHubRegistryManager, \
    receive_twin_reported, \
    twin_desired, \
    clear_desired_twin, \
    clear_blob_storage, \
    read_blobs


async def main():
    # Подключение к IoTHub


    CONNECTION_STRING_MANAGER = "HostName=IOT-projekt-Biaheza.azure-devices.net;SharedAccessKeyName=PyCharm_code;SharedAccessKey=hD/zh4IK/3DTFGrCVO8+0WheDbA9C/6doAIoTLSnmf4="
    DEVICE_ID = "Device1"

    iothub_registry_manager = IoTHubRegistryManager(CONNECTION_STRING_MANAGER)


    # Очистка желаемого twin
    await clear_desired_twin(iothub_registry_manager, DEVICE_ID)

    account_name = 'informationpoduction'
    account_key = 'XJ2jFej6iQzCvd56jPf0sEWRrz+A7GdfkDkET7powUttQ6vrRmBGyxgZmthd1Ys2LMdKMTUychPy+AStRAxLPw=='


    STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

    # Очистка хранилища blob
    await clear_blob_storage(STORAGE_CONNECTION_STRING)

    old_date_err = ""
    old_date_kpi = ""

    try:
        while True:
            # получение twin reported
            twin_reported = await receive_twin_reported(iothub_registry_manager, DEVICE_ID)

            # отправка желаемого twin
            await twin_desired(iothub_registry_manager, DEVICE_ID, twin_reported)

            # чтение хранилища blob и обновление даты новых блобов
            new_date_err, new_date_kpi = await read_blobs(iothub_registry_manager,
                                                                         DEVICE_ID,
                                                                         STORAGE_CONNECTION_STRING,
                                                                         old_date_err,
                                                                         old_date_kpi)

            # обновление
            old_date_err = new_date_err
            old_date_kpi = new_date_kpi

            time.sleep(1)
    except Exception as e:
        print("Progam is stopeed")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
