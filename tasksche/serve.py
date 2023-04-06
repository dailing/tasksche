import flask_socketio
from flask import Flask, request
from flask_socketio import SocketIO

from tasksche.run import get_logger

logger = get_logger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

list_rooms = dict()


@socketio.on('connect')
def test_connect():
    print('connected')


@socketio.on('disconnect')
def test_disconnect():
    print('Client disconnected')


@socketio.on('join')
def join_room(json):
    room = json['room']
    flask_socketio.join_room(room)
    print(request)
    print(request.sid)
    list_rooms[room] = request.sid
    # logger.info(flask_socketio.call('get_task', namespace=json['room']))
    return dict(msg='OK')


@app.route('/client/rooms')
def client_rooms_get():
    return dict(rooms=list(list_rooms))


@app.route('/client/tasks/<task_name>')
def client_tasks_get(task_name):
    logger.info(task_name)
    return flask_socketio.call('get_task', to=list_rooms[task_name], timeout=1, namespace=None)


if __name__ == '__main__':
    logger.info('FFUCK')
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, port=6000)
