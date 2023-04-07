<template>
    <!--  <img alt="Vue logo" src="./assets/logo.png">-->
    <HelloWorld msg="Welcome to Your Vue.js App"/>
</template>

<script>
import HelloWorld from './components/HelloWorld.vue'

export default {
    name: 'App',
    components: {
        HelloWorld
    },
    mounted() {
        console.log(this.$socket);
        console.log('fuck');
    },
    sockets: {
        connect() {
            console.log('socket connected')
        },
    }

}
</script>

<style>
@import 'https://cdn.jsdelivr.net/npm/@vue-flow/core@1.18.1/dist/style.css';
@import 'https://cdn.jsdelivr.net/npm/@vue-flow/core@1.18.1/dist/theme-default.css';
@import 'https://cdn.jsdelivr.net/npm/@vue-flow/controls@latest/dist/style.css';
@import 'https://cdn.jsdelivr.net/npm/@vue-flow/minimap@latest/dist/style.css';
@import 'https://cdn.jsdelivr.net/npm/@vue-flow/node-resizer@latest/dist/style.css';

#app {
    font-family: Avenir, Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    color: #2c3e50;
    margin-top: 60px;
}
</style>
