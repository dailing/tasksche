import flask_socketio
from flask import Flask, request
from flask_socketio import SocketIO

from tasksche.run import get_logger

logger = get_logger(__name__)

app = Flask(__name__)
logger.info(app.root_path)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, logger=logger)

list_rooms = dict()
sid_to_room = dict()


@socketio.on('connect')
def test_connect():
    logger.info('connected')


@socketio.on('disconnect')
def test_disconnect():
    logger.info('Client disconnected')


@socketio.on('join')
def join_room(json):
    room = json['room']
    flask_socketio.join_room(room)
    logger.info(request.sid)
    list_rooms[room] = request.sid
    sid_to_room[request.sid] = room
    # logger.info(flask_socketio.call('get_task', namespace=json['room']))
    return dict(msg='OK')


@socketio.on('graph_change')
def on_socket_graph_change():
    room = sid_to_room.get(request.sid, None)
    if room is None:
        logger.info(f'{request.sid} not in room list!')
    logger.info(f'graph change {room}')
    socketio.emit('graph_change_to_web')


@app.route('/client/rooms')
def client_rooms_get():
    return dict(rooms=list(list_rooms))


@app.route('/')
def index():
    return "FUCK"


@app.route('/client/tasks/<task_name>')
def client_tasks_get(task_name):
    logger.info(task_name)
    return flask_socketio.call('get_task', to=list_rooms[task_name], timeout=1, namespace=None)


if __name__ == '__main__':
    logger.info('FFUCK')
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, port=12345)
    # app.run(port=6000, debug=True)
