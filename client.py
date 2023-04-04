import socketio

# standard Python
sio = socketio.Client()

# asyncio
# sio = socketio.AsyncClient()


@sio.on('my message')
def on_message(data):
    print('I received a message!', data)


sio.connect('http://localhost:5000')
