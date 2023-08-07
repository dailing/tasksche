import axios from 'axios'
import VueAxios from 'vue-axios'
import {createApp} from 'vue';
import App from './App.vue'
import {io} from 'socket.io-client';
import VueSocketIOExt from "vue-socket.io-extended";
import Antd from 'ant-design-vue';
// import App from './App';
import 'ant-design-vue/dist/reset.css';

var app = createApp(App);
app.use(VueAxios, axios);
const socket = io();
app.use(VueSocketIOExt, socket);
app.use(Antd);
app.mount('#app');
