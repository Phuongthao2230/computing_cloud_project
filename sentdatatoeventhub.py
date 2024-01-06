#Import các thư viện cần thiết
import json
import yfinance as yf
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import asyncio 
from datetime import datetime 

#Thông số lấy từ azure
connection_str = 'Endpoint=sb://assigneventhubs.servicebus.windows.net/;SharedAccessKeyName=MyPolicy;SharedAccessKey=ouxq8uXi4+VMrNioPIo7EgraY4fpbSmFk+AEhNQkvfM=;EntityPath=stockeventhubs'
eventhub_name = 'stockeventhubs' 

#Hàm tính rsi
def calculate_rsi(data):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    data['RSI'] = rsi
    return data

async def send_realtime_data_to_eventhub():
    try:
        producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
        async with producer:
            while True:
                
                stock_data = yf.download("MSFT")

                # Tính toán chỉ số RSI từ dữ liệu cổ phiếu
                stock_data = calculate_rsi(stock_data)

                # Thêm trường mới (ví dụ: thời gian lấy dữ liệu)
                stock_data['DataRetrievedTime'] = datetime.now()

                # Chia dữ liệu thành các chunk nhỏ hơn và gửi lên Event Hub
                chunk_size = 1000  # Đặt kích thước chunk mong muốn
                for start in range(0, len(stock_data), chunk_size):
                    chunk = stock_data.iloc[start:start+chunk_size].to_json(orient='records')
                    event_data_batch = await producer.create_batch()
                    event_data_batch.add(EventData(chunk))
                    await producer.send_batch(event_data_batch)
                    print("Sent analyzed data to Event Hub.")

                # Chờ một khoảng thời gian trước khi lấy dữ liệu tiếp theo (30 giây)
                await asyncio.sleep(30)
    except Exception as err:
        print(f"Error: {err}")

# Gọi hàm gửi dữ liệu cổ phiếu real-time lên Event Hub với biến đổi và phân tích
asyncio.run(send_realtime_data_to_eventhub())