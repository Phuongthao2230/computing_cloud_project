import yfinance as yf
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import asyncio 
import pandas as pd
from azure.storage.blob.aio import BlobServiceClient

# Cấu hình kết nối Event Hub, Blob Storage và thông tin khác
eventhub_connection_str = 'Endpoint=sb://assigneventhubs.servicebus.windows.net/;SharedAccessKeyName=MyPolicy;SharedAccessKey=ouxq8uXi4+VMrNioPIo7EgraY4fpbSmFk+AEhNQkvfM=;EntityPath=stockeventhubs'  
eventhub_name = 'stockeventhubs'
blob_connection_str = 'DefaultEndpointsProtocol=https;AccountName=assignment04storages;AccountKey=VFvZLLHX26xED4kdMuvx/s4fRnsQ2DiaaWivVkR6N1w7JKc31boTMg54wMF72dKQucUYJW87fXxG+AStBRCcmA==;EndpointSuffix=core.windows.net'
container_name = 'assignblob'
blob_path = 'stockdata.csv'

async def save_to_blob_storage_as_binary(data):
    blob_service_client = BlobServiceClient.from_connection_string(blob_connection_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    data_bytes = data.encode('utf-8')
    await blob_client.upload_blob(data_bytes, overwrite=True)

async def send_realtime_data_to_eventhub():
    try:
        producer = EventHubProducerClient.from_connection_string(eventhub_connection_str, eventhub_name=eventhub_name)
        async with producer:
            while True:
                stock_data = yf.download("MSFT")
                stock_data = stock_data.reset_index()

                # Chuyển đổi DataFrame sang CSV
                csv_data = stock_data.to_csv(index=False)

                # Lưu dữ liệu CSV dưới dạng binary vào Blob Storage
                await save_to_blob_storage_as_binary(csv_data)

                # Gửi dữ liệu CSV dưới dạng binary lên Event Hub
                event_data_batch = await producer.create_batch()
                event_data_batch.add(EventData(csv_data.encode('utf-8')))
                await producer.send_batch(event_data_batch)
                print("Sent analyzed data to Event Hub.")

                # Chờ một khoảng thời gian trước khi lấy dữ liệu tiếp theo (ví dụ: 1 phút)
                await asyncio.sleep(30)
    except Exception as err:
        print(f"Error: {err}")

# Gọi hàm gửi dữ liệu cổ phiếu real-time lên Event Hub và lưu vào Blob Storage dưới dạng binary
asyncio.run(send_realtime_data_to_eventhub())