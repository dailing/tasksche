import axios from 'axios'
import VueAxios from 'vue-axios'
import {createApp} from 'vue';
import App from './App.vue'
import {io} from 'socket.io-client';
import VueSocketIOExt from "vue-socket.io-extended";

var app = createApp(App);
app.use(VueAxios, axios)
const socket = io();
app.use(VueSocketIOExt, socket);
app.mount('#app');
