import asyncio
import logging
from datetime import datetime
import random

# Настройка логирования (сохранение в файл и вывод в консоль)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("chat_server.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class ChatServer:
    def __init__(self, host='127.0.0.1', port=8888):
        self.host = host
        self.port = port
        self.clients = {}  # Словарь подключенных клиентов {writer: username}

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')

        # Запрос имени пользователя (Авторизация)
        writer.write(b"Enter your username: ")
        await writer.drain()

        username_data = await reader.readline()
        if not username_data:
            writer.close()
            await writer.wait_closed()
            return

        username = username_data.decode('utf-8').strip()
        if not username:
            username = f"Guest_{addr[1]}"

        self.clients[writer] = username
        logging.info(f"User '{username}' connected from {addr}")

        # Уведомление остальных о новом пользователе
        join_msg = f"--- {username} has joined the chat ---\n"
        await self.broadcast(join_msg, exclude_writer=writer)

        # Приветствие нового пользователя
        welcome_msg = f"Welcome to the chat, {username}! Type /help for commands.\n"
        writer.write(welcome_msg.encode('utf-8'))
        await writer.drain()

        try:
            while True:
                data = await reader.readline()
                if not data:
                    break

                message = data.decode('utf-8').strip()
                if not message:
                    continue

                logging.info(f"[{username}]: {message}")

                # Обработка команд чата
                if message.startswith('/'):
                    await self.handle_command(message, writer, username)
                else:
                    # Форматирование и трансляция обычного сообщения
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    formatted_msg = f"[{timestamp}] {username}: {message}\n"
                    await self.broadcast(formatted_msg)

        except asyncio.CancelledError:
            pass
        except (ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception as e:
            logging.error(f"Error handling client {username}: {e}")
        finally:
            # Обработка отключения пользователя
            if writer in self.clients:
                del self.clients[writer]
            if not writer.is_closing():
                writer.close()
            await writer.wait_closed()

            leave_msg = f"--- {username} has left the chat ---\n"
            await self.broadcast(leave_msg)
            logging.info(f"User '{username}' disconnected")

    async def handle_command(self, command, writer, username):
        cmd = command.lower()
        if cmd == '/help':
            help_text = (
                "Available commands:\n"
                "/help - Show this help message\n"
                "/users - List connected users\n"
                "/random - Get a random quote\n"
                "/quit - Disconnect from the server\n"
            )
            writer.write(help_text.encode('utf-8'))
            await writer.drain()
        elif cmd == '/users':
            users = list(self.clients.values())
            users_text = f"Connected users ({len(users)}): {', '.join(users)}\n"
            writer.write(users_text.encode('utf-8'))
            await writer.drain()
        elif cmd == '/random':
            quotes = [
                "The only way to do great work is to love what you do. - Steve Jobs",
                "In the middle of difficulty lies opportunity. - Albert Einstein",
                "Simplicity is the ultimate sophistication. - Leonardo da Vinci",
                "Code is like humor. When you have to explain it, it’s bad. - Cory House"
            ]
            quote = random.choice(quotes)
            writer.write(f"Random quote: {quote}\n".encode('utf-8'))
            await writer.drain()
        elif cmd == '/quit':
            writer.close()
        else:
            writer.write(f"Unknown command: {command}. Type /help for a list of commands.\n".encode('utf-8'))
            await writer.drain()

    async def broadcast(self, message, exclude_writer=None):
        for client_writer, uname in list(self.clients.items()):
            if client_writer != exclude_writer and not client_writer.is_closing():
                try:
                    client_writer.write(message.encode('utf-8') if isinstance(message, str) else message)
                    await client_writer.drain()
                except Exception as e:
                    logging.error(f"Error broadcasting to {uname}: {e}")

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        addr = server.sockets[0].getsockname()
        logging.info(f"Chat server started on {addr}")

        async with server:
            await server.serve_forever()


if __name__ == '__main__':
    chat_server = ChatServer(host='127.0.0.1', port=9999)
    try:
        asyncio.run(chat_server.start())
    except KeyboardInterrupt:
        logging.info("Server shutting down.")


















