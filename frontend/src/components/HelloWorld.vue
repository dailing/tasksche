<template>
    <a-layout has-sider>
        <a-layout-sider
                :style="{ overflow: 'auto', height: '100vh', position: 'fixed', left: 0, top: 0, bottom: 0 }"
        >
            <a-menu v-model:selectedKeys="selectedKeys" theme="dark" mode="inline">
                <a-menu-item key="1">
                    <user-outlined/>
                    <span class="nav-text">nav 1</span>
                </a-menu-item>
                <a-menu-item key="2">
                    <video-camera-outlined/>
                    <span class="nav-text">nav 2</span>
                </a-menu-item>
            </a-menu>
        </a-layout-sider>
        <a-layout :style="{ marginLeft: '200px' }">
<!--            <a-layout-header :style="{ background: '#fff', padding: 0 }"/>-->
            <a-layout-content :style="{ margin: '0px 0px 0', overflow: 'initial' }">
                <input type="text" placeholder="'name" v-model="task_name">
                <input type="button" v-on:click="click" value="refresh">

                <div style="height: 1200px; width: 100%">
                    <VueFlow v-model="element" @node:selected="onNodeSelected"/>
                </div>
            </a-layout-content>
        </a-layout>
    </a-layout>
</template>

<script>
import {VueFlow} from '@vue-flow/core'
import axios from "axios";

export default {
    name: 'HelloWorld',
    props: {},
    components: {
        VueFlow
    },
    data() {
        return {
            element: [],
            task_name: 'export',
        }
    },
    mounted() {
        this.refresh_graph();
    },
    computed: {},
    methods: {
        click() {
            console.log('fuck');
            this.refresh_graph();
        },
        onNodeSelected(node) {
            node.style = {
                ...node.style,
                backgroundColor: 'yellow',
                fontWeight: 'bold',
                lineWidth: 3,
            };
            console.log(node.id);
        },
        refresh_graph() {
            axios.get('/client/tasks/' + this.task_name).then((resp) => {
                console.log(resp);
                let new_map = resp.data;

                // delete none existing keys, updating new keys
                let ori_keys = new Set();
                for (const k of this.element) {
                    ori_keys.add(k.id)
                }
                for (let i = 0; i < this.element.length; i += 1) {
                    while (!(this.element[i].id in new_map)) {
                        this.element.splice(i, 1);
                    }
                    let kk = new_map[this.element[i].id];
                    delete (kk['position']);
                    Object.assign(this.element[i], kk);
                }
                // adding new elements
                for (const k in new_map) {
                    if (!ori_keys.has(k)) {
                        this.element.push(new_map[k])
                    }
                }

            })
        },
    },
    sockets: {
        graph_change_to_web() {
            console.log('graph change');
            this.refresh_graph();
        },

    }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped></style>
