import socketio
import asyncio
# import watchfiles

# asyncio
sio = socketio.AsyncClient()


@sio.on('my message')
def on_message(data):
    print('I received a message!', data)


async def main():
    await sio.connect('http://localhost:6000')
    # notifier = pyinotify.AsyncioNotifier(wm, loop,
    #                                      callback=handle_read_callback)
    # while True:
    #     await asyncio.sleep(1)
    #     print(1)


def handle_read_callback(notifier):
    """
    Just stop receiving IO read events after the first
    iteration (unrealistic example).
    """
    print('handle_read callback')
    # notifier.loop.stop()


if __name__ == '__main__':
    wm = pyinotify.WatchManager()
    loop = asyncio.get_event_loop()
    notifier = pyinotify.AsyncioNotifier(
        wm, loop,
        callback=handle_read_callback)
    wm.add_watch('/tmp', pyinotify.ALL_EVENTS)
    asyncio.run(main())
    loop.run_forever()
