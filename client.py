import asyncio
import sys


async def read_messages(reader):
    try:
        while True:
            data = await reader.readline()
            if not data:
                break
            # Печатаем сообщение от сервера
            print(data.decode('utf-8').strip(), flush=True)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"\nConnection lost: {e}")


async def send_messages(writer):
    try:
        loop = asyncio.get_running_loop()
        while True:
            # Чтение ввода пользователя в неблокирующем режиме
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            message = line.strip()
            if message:
                # Обработка команды /quit локально
                if message.lower() == '/quit':
                    print("Disconnecting from the server...", flush=True)
                    writer.close()
                    await writer.wait_closed()
                    break
                writer.write((message + '\n').encode('utf-8'))
                await writer.drain()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"\nError sending message: {e}")


async def start_client(host='127.0.0.1', port=9999):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print("Error: Could not connect to the server. Is it running?")
        return

    print("Connected to the server.")

    # Чтение запроса имени пользователя
    print("Waiting for server response...", flush=True)
    try:
        prompt_data = await asyncio.wait_for(reader.readline(), timeout=5.0)
        if prompt_data:
            print(prompt_data.decode('utf-8').strip(), end=" ", flush=True)
        else:
            print("No response from server (empty data).", flush=True)
    except asyncio.TimeoutError:
        print("Timeout: No response from server within 5 seconds.", flush=True)

    loop = asyncio.get_running_loop()
    username = (await loop.run_in_executor(None, sys.stdin.readline)).strip()
    if not username:
        username = "Anonymous"

    writer.write((username + '\n').encode('utf-8'))
    await writer.drain()

    # Чтение приветственного сообщения
    welcome_data = await reader.readline()
    print(welcome_data.decode('utf-8').strip(), flush=True)

    # Запуск параллельных задач для чтения и отправки сообщений
    read_task = asyncio.create_task(read_messages(reader))
    write_task = asyncio.create_task(send_messages(writer))

    # Ожидаем завершения любой из задач (например, отключения сервера)
    done, pending = await asyncio.wait(
        [read_task, write_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()

    writer.close()
    await writer.wait_closed()
    print("Disconnected from the server.")


if __name__ == '__main__':
    try:
        asyncio.run(start_client())
    except KeyboardInterrupt:
        print("\nClient shutting down.")
